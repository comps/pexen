import sys
from collections import namedtuple
import threading
import queue
import multiprocessing
import multiprocessing.queues  # for picklability check
import pickle

from . import common, meta


class PoolError(common.SchedulerError):
    """Raised by ProcessWorkerPool or ThreadWorkerPool."""
    pass

def _is_picklable(obj):
    try:
        pickle.dumps(obj)
        return True
    except (TypeError, AttributeError):
        return False

# used internally by WorkerPool classes
_PoolWorkerMsg = namedtuple('_PoolWorkerMsg', ['workid', 'type', 'taskidx',
                                               'shared', 'ret', 'excinfo'])
# TODO: use Python 3.7 namedtuple defaults
_PoolWorkerMsg.__new__.__defaults__ = (None,) * len(_PoolWorkerMsg._fields)

# TODO: also document that by delaying iteration of iter_results, the user
#       can effectively throttle the execution as no new items will be scheduled
#       until the user gives us back control
class ProcessWorkerPool:
    _Queue = multiprocessing.Queue
    _Worker = multiprocessing.Process

    def __init__(self, workers=1, spare=1):
        self.wanted_workers = workers
        self.wanted_spare = spare
        self.unfinished_workers = 0
        self.taskq = self._Queue()
        self.resultq = self._Queue()
        self.active_tasks = 0
        self.shutting_down = False
        self.workers = []
        # this serves two purposes:
        # - it lets us pass a picklable integer into the worker instead of
        #   a potentially unpicklable callable
        # - it lets the worker return a picklable integer, which we then
        #   transform back to the callable before returning it to the user
        self.taskmap = {}

    def _worker_body(self, workid, outqueue, inqueue):
        while True:
            taskinfo = inqueue.get()
            if taskinfo is None:
                msg = _PoolWorkerMsg(workid, 'finished')
                outqueue.put(msg)
                break
            try:
                taskidx, shared = taskinfo
                task = self.taskmap[taskidx]
                kwargs = meta.get_kwargs(task)
                ret = None
                # support special case: argument-less task, for simplicity
                if task.__code__.co_argcount == 0:
                    ret = task(**kwargs)
                else:
                    ret = task(shared, **kwargs)
                if isinstance(outqueue, multiprocessing.queues.Queue):
                    if not _is_picklable(ret):
                        raise AttributeError("Can't pickle callable return value")
                    if not _is_picklable(shared):
                        raise AttributeError("Can't pickle callable shared state")
                msg = _PoolWorkerMsg(workid, 'taskdone', taskidx, shared, ret)
                outqueue.put(msg)
            except Exception:
                extype, exval, extb = sys.exc_info()
                # multiprocessing.Queue prints a TypeError if an object is not
                # picklable, but doesn't actually raise an exception, so we need
                # to check picklability manually
                if isinstance(outqueue, multiprocessing.queues.Queue):
                    if not _is_picklable(ret):
                        ret = None
                    if not _is_picklable(shared):
                        shared = None
                    if not _is_picklable(exval):
                        exval = None
                    if not _is_picklable(extb):
                        extb = None
                msg = _PoolWorkerMsg(workid, 'taskdone', taskidx, shared, ret,
                                     common.ExceptionInfo(extype, exval, extb))
                outqueue.put(msg)

                # in case the above ever gets fixed:
                #try:
                #    outqueue.put((taskidx, None, (extype, exval, extb)))
                #except TypeError:
                #    try:
                #        outqueue.put((taskidx, None, (extype, exval, None)))
                #    except TypeError:
                #        outqueue.put((taskidx, None, (extype, None, None)))

    def start(self):
        if self.unfinished_workers > 0:
            raise PoolError("Cannot re-start a running pool")
        self.workers = []
        for workid in range(self.wanted_workers):
            w = self._Worker(target=self._worker_body,
                             args=(workid, self.resultq, self.taskq))
            w.start()
            self.workers.append(w)
        self.unfinished_workers = self.wanted_workers
        self.max_tasks = self.wanted_workers + self.wanted_spare
        self.shutting_down = False

    def full(self):
        return self.active_tasks >= self.max_tasks

    def empty(self):
        return self.active_tasks <= 0

    def idlecnt(self):
        return self.max_tasks - self.active_tasks

    # self.unfinished_workers represent how many workers owe us
    # msg.type==finished so that we know whether we should wait for more task
    # results to come from the queue in self.iter_results() or return to user
    # - this will be 0 if the user exhausted all iter_results()
    # - but it will be non-0 if the user just called shutdown(wait=True)
    #
    # self.alive represents how many actual OS threads or processes are running
    # so we know when it's safe to modify the shared non-fork()ed state
    # - this will be 0 regardless of whether the user called iter_results()
    # - it will be 0 after shutdown(wait=True) returns
    # - it may be non-0 right after shutdown(wait=False), but it will itself
    #   eventually become 0 as the workers exit when finished

    def alive(self):
        return any((x.is_alive() for x in self.workers))

    def register(self, tasks):
        if self.alive():
            raise PoolError("Cannot register tasks while the pool is running")
        for task in tasks:
            self.taskmap[id(task)] = task

    # mp:  just check + raise if not present
    # thr: just update, don't check
    def _check_update_taskmap(self, task):
        if id(task) not in self.taskmap:
            raise PoolError(f"Cannot submit unregistered task {task}")

    def submit(self, task, shared={}):
        if self.shutting_down:
            raise PoolError("The pool is shutting down")
        if not self.alive():
            raise PoolError("The pool is not running")
        self._check_update_taskmap(task)
        self.taskq.put((id(task), shared))
        self.active_tasks += 1

    def shutdown(self, wait=False):
        if self.shutting_down:
            raise PoolError("The pool is already shutting down")
        self.shutting_down = True
        for _ in range(len(self.workers)):
            self.taskq.put(None)
        if wait:
            for w in self.workers:
                w.join()
            # iter_results over queued results still valid after this

    def iter_results(self):
        while self.unfinished_workers > 0 or self.active_tasks > 0:
            msg = self.resultq.get()
            if msg.type == 'finished':
                self.workers[msg.workid].join()
                self.unfinished_workers -= 1
            elif msg.type == 'taskdone':
                task = self.taskmap[msg.taskidx]
                self.active_tasks -= 1
                yield common.TaskRes(task, msg.shared, msg.ret, msg.excinfo)
            else:
                raise RuntimeError(f"unexpected msg: {msg}")

class ThreadWorkerPool(ProcessWorkerPool):
    _Queue = queue.Queue
    _Worker = threading.Thread

    def register(self, tasks):
        pass

    def _check_update_taskmap(self, task):
        self.taskmap[id(task)] = task
