import sys
from collections import namedtuple
import threading
import queue
import multiprocessing
import multiprocessing.queues  # for picklability check
import pickle

from .shared import SchedulerError
from .meta import get_kwargs


class PoolError(SchedulerError):
    """Raised by ProcessWorkerPool or ThreadWorkerPool."""
    pass

def _is_picklable(obj):
    try:
        pickle.dumps(obj)
        return True
    except (TypeError, AttributeError):
        return False

# used internally by WorkerPool classes
_PoolWorkerMsg = namedtuple('_PoolWorkerMsg', ['workid', 'msgtype', 'taskidx',
                                               'shared', 'ret', 'excinfo'])
# TODO: use Python 3.7 namedtuple defaults
_PoolWorkerMsg.__new__.__defaults__ = (None,) * len(_PoolWorkerMsg._fields)

# taskres returned to the caller
PoolTaskRes = namedtuple('PoolTaskRes', ['task', 'shared', 'ret', 'excinfo'])

# TODO: also document that by delaying iteration of iter_results, the user
#       can effectively throttle the execution as no new items will be scheduled
#       until the user gives us back control
class ProcessWorkerPool:
    _Queue = multiprocessing.Queue
    _Worker = multiprocessing.Process

    def __init__(self, alltasks, workers=1, spare=1):
        self.alive_workers = 0
        self.taskq = self._Queue()
        self.resultq = self._Queue()
        self.active_tasks = 0
        # this serves two purposes:
        # - it lets us pass a picklable integer into the worker instead of
        #   a potentially unpicklable callable
        # - it lets the worker return a picklable integer, which we then
        #   transform back to the callable before returning it to the user
        self.task_map = {}
        self.start_pool(workers, spare, alltasks)

    @staticmethod
    def _worker_body(workid, alltasks, outqueue, inqueue):
        while True:
            taskinfo = inqueue.get()
            if taskinfo is None:
                msg = _PoolWorkerMsg(workid, 'finished')
                outqueue.put(msg)
                break
            try:
                taskidx, shared = taskinfo
                task = alltasks[taskidx]
                kwargs = get_kwargs(task)
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
                                    (extype, exval, extb))
                outqueue.put(msg)

                # in case the above ever gets fixed:
                #try:
                #    outqueue.put((taskidx, None, (extype, exval, extb)))
                #except TypeError:
                #    try:
                #        outqueue.put((taskidx, None, (extype, exval, None)))
                #    except TypeError:
                #        outqueue.put((taskidx, None, (extype, None, None)))

    # TODO: document the idea of 'spare'; a buffer of pre-scheduled tasks ready
    #       to be executed and automatically picked by the workers without any
    #       work from the parent process (without iter_results needing to run);
    #       used exactly like make -j5 (on 4-core machine), to avoid worker
    #       starvation when the parent is busy processing results or scheduling
    #       new tasks
    #       - too small value can result in starvation if the tasks execute too
    #         quickly and result processing takes a lot of time
    #       - too large value causes suboptimal ordering; high priority tasks
    #         are scheduled *after* these queued tasks, any mutexes (claims) are
    #         held even if the queued task is not yet running, etc.

    def start_pool(self, workers=1, spare=1, alltasks=None):
        if self.alive_workers > 0:
            raise PoolError("Cannot re-start a running pool")
        if alltasks:
            self.task_map = dict(((id(t), t) for t in alltasks))
        self.workers = []
        for workid in range(workers):
            w = self._Worker(target=self._worker_body,
                             args=(workid, self.task_map,
                                   self.resultq, self.taskq))
            w.start()
            self.workers.append(w)
        self.alive_workers = workers
        self.max_tasks = workers + spare
        self.shutting_down = False

    def full(self):
        return self.active_tasks >= self.max_tasks

    def empty(self):
        return self.active_tasks <= 0

    def idlecnt(self):
        return self.max_tasks - self.active_tasks

    def submit(self, task, shared=None):
        if self.shutting_down:
            raise PoolError("Cannot submit tasks, the pool is shutting down")
        if id(task) not in self.task_map:
            raise PoolError(f"Cannot submit unknown task {task}, "
                            "it was not provided before pool start.")
        self.taskq.put((id(task), shared))
        self.active_tasks += 1

    def shutdown(self, wait=False):
        if self.shutting_down:
            return
        self.shutting_down = True
        for _ in range(len(self.workers)):
            self.taskq.put(None)
        if wait:
            for w in self.workers:
                w.join()
            # iter_results over queued results still valid after this

    # if you need something asynchronous instead of the blocking iter_results,
    # ie. something returning a Queue, you may need to spawn a new thread to
    # manage the queue

    # TODO: document that iter_results can be called multiple times to get
    #       multiple iterators
    #       also document that they are NOT thread or process safe
    def iter_results(self):
        while self.alive_workers > 0 or self.active_tasks > 0:
            msg = self.resultq.get()
            if msg.msgtype == 'finished':
                self.workers[msg.workid].join()
                self.alive_workers -= 1
            elif msg.msgtype == 'taskdone':
                task = self.task_map[msg.taskidx]
                self.active_tasks -= 1
                yield PoolTaskRes(task, msg.shared, msg.ret, msg.excinfo)
            else:
                raise RuntimeError(f"unexpected msg: {msg}")

class ThreadWorkerPool(ProcessWorkerPool):
    _Queue = queue.Queue
    _Worker = threading.Thread

