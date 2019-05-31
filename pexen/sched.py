import sys
import warnings
from collections import namedtuple
import threading
import queue
import multiprocessing
import multiprocessing.queues  # for picklability check
import pickle
#from collections import deque

# No. as_completed() or wait() don't return results as soon as they finish,
# they just iterate the list of futures from front to back, .. use a real pool
#from concurrent import futures

from .attr import get_requires, get_provides, get_kwargs, get_priority
from . import util

class SchedulerError(util.PexenError):
    pass

class DepcheckError(SchedulerError):
    """Raised by the scheduler when a requires/provides check fails.

    The arg contains the remaining unsatisfied dependency metadata.
    """
    def __init__(self, msg, remain):
        self.remain = remain
        super().__init__(msg)

class PoolError(SchedulerError):
    """Raised by ProcessWorkerPool or ThreadWorkerPool."""
    pass


class ProcessWorkerPool:
    _Queue = multiprocessing.Queue
    _Worker = multiprocessing.Process

    _WorkerMsg = namedtuple('WorkerMsg', ['workid', 'type', 'taskid', 'ret', 'exc'])

    def __init__(self, workers=1, spare=1):
        self.alive_workers = 0
        self.taskq = self._Queue()
        self.resultq = self._Queue()
        self.active_tasks = 0
        self.max_tasks = workers+spare
        self.start_pool(workers)

    @staticmethod
    def _worker_body(workid, outqueue, inqueue):
        # avoid using WorkerMsg here: it's not picklable
        # if this body becomes too complex, make this a @classmethod and transfer
        # namedtuple._asdict().values() across to the parent
        while True:
            taskinfo = inqueue.get()
            if not taskinfo:
                msg = (workid, 'finished', None, None, None)
                outqueue.put(msg)
                break
            try:
                taskid, task, shared = taskinfo
                kwargs = get_kwargs(task)
                ret = task(shared, **kwargs)
                msg = (workid, 'taskdone', taskid, ret, None)
                outqueue.put(msg)
            except Exception:
                extype, exval, extb = sys.exc_info()
                # multiprocessing.Queue prints a TypeError if an object is not
                # picklable, but doesn't actually raise an exception, so we need
                # to check picklability manually
                if isinstance(outqueue, multiprocessing.queues.Queue):
                    try:
                        pickle.dumps(exval)
                    except TypeError:
                        exval = None
                    try:
                        pickle.dumps(extb)
                    except TypeError:
                        extb = None
                msg = (workid, 'taskdone', taskid, None, (extype, exval, extb))
                outqueue.put(msg)

                # in case the above ever gets fixed:
                #try:
                #    outqueue.put((taskid, None, (extype, exval, extb)))
                #except TypeError:
                #    try:
                #        outqueue.put((taskid, None, (extype, exval, None)))
                #    except TypeError:
                #        outqueue.put((taskid, None, (extype, None, None)))

    def start_pool(self, workers):
        if self.alive_workers > 0:
            raise PoolError("Cannot re-start a running pool")
        self.workers = []
        for workid in range(workers):
            w = self._Worker(target=self._worker_body,
                             args=(workid, self.resultq, self.taskq))
            w.start()
            self.workers.append(w)
        self.alive_workers = workers
        self.task_index = {}  # so we don't pickle task objects
        self.shutting_down = False

    def full(self):
        return self.active_tasks >= self.max_tasks

    def empty(self):
        return self.active_tasks <= 0

    def idlecnt(self):
        return self.max_tasks - self.active_tasks

    def submit(self, task, shared):
        if self.shutting_down:
            raise PoolError("Cannot submit tasks, the pool is shutting down")
        self.task_index[id(task)] = task
        self.taskq.put((id(task), task, shared))
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
    # manage the queue and call active_tasks -= 1 for each added item

    # TODO: document that iter_results can be called multiple times to get
    #       multiple iterators, each being thread-safe or process-safe, depending
    #       on the WorkerPool type
    def iter_results(self):
        while self.alive_workers > 0 or self.active_tasks > 0:
            msg = self._WorkerMsg._make(self.resultq.get())
            if msg.type == 'finished':
                self.workers[msg.workid].join()
                self.alive_workers -= 1
            elif msg.type == 'taskdone':
                task = self.task_index[msg.taskid]
                self.active_tasks -= 1
                yield (task, msg.ret, msg.exc)
            else:
                raise RuntimeError(f"unexpected msg: {msg}")

class ThreadWorkerPool(ProcessWorkerPool):
    _Queue = queue.Queue
    _Worker = threading.Thread

class Sched:
    def __init__(self, tasks=set()):
        # all added callables
        self.tasks = set()
        # provided resources pointing to tasks requiring them
        self.deps = {}
        # default for shared state between chained tasks
        self.default_shared = {}
        # whether the current task set was sanity checked
        self.checked = False
        if tasks:
            self.add_tasks(tasks)

    def add_tasks(self, new):
        self.tasks.update(set(new))
        self.checked = False
        if __debug__:
            self.check()
        # save a few cycles - if the check fails, don't build deps for execution
        self._build_deps(new, self.deps)

    def _build_deps(self, tasks, alldeps={}):
        """Build a dict of task dependencies (requires/provides).

        Top-level dict is indexed by dep name, each having a (sub-)dict indexed
        by task callable, each of which points to a set of requires of the task.
        """
        for task in tasks:
            reqs = get_requires(task).copy()
            for req in reqs:
                if req in alldeps:
                    alldeps[req][task] = reqs
                else:
                    alldeps[req] = {task: reqs}
        return alldeps

    @staticmethod
    def _satisfy_provides(alldeps, task):
        """Go through a task's provides and satisfy all requires in alldeps.

        Returns a tuple of two sets; tasks ready to be run (all requires
        satisfied) and all affected ("child") tasks, even the ones with still
        unmet requires.
        """
        torun = set()
        children = set()
        for provide in get_provides(task):
            if provide not in alldeps:
                warnings.warn(f"Dep \"{provide}\" provided by {task}, "
                              "but not required by any task",
                              category=util.PexenWarning)
                continue
            ctasks = alldeps.pop(provide)
            children.update(ctasks)
            for ctask, creqs in ctasks.items():
                creqs.remove(provide)
                if not creqs:
                    torun.add(ctask)
        return (torun, children)

    def _simulate_deps(self):
        alldeps = self._build_deps(self.tasks)
        frontline = list((t for t in self.tasks if not get_requires(t)))
        for task in frontline:
            print(f"frontline: {frontline}")
            # imagine we run the task <here>
            # now satisfy deps by this task's provides
            nexttasks, children = self._satisfy_provides(alldeps, task)
            frontline.extend(nexttasks)
        if alldeps:
            raise util.DepcheckError(
                f"Unsatisfied requires remain: {list(alldeps.keys())}",
                alldeps)

    def check(self):
        """Run preliminary safety checks before execution.

        Performed automatically unless running python with -O.
        """
        if not self.checked:
            self._simulate_deps()
        self.checked = True

    def add_shared(self, **kwargs):
        """Pre-set key/values in the default shared space."""
        self.default_shared.update(kwargs)

    def run_simple(self):
        # tasks without requires
        frontline = list((t for t in self.tasks if not get_requires(t)))
        allshared = dict(((task, self.default_shared.copy()) for task in self.tasks))
        for task in frontline:
            shared = allshared[task]
            kwargs = get_kwargs(task)
            yield task(shared, **kwargs)
            nexttasks, children = self._satisfy_provides(self.deps, task)
            for child in children:
                allshared[child].update(allshared[task])
            del allshared[task]
            frontline.extend(nexttasks)

    @staticmethod
    def _annotate_prio(callobj, cnt):
        primary = get_priority(callobj)
        # give each callobj a unique integer, to prevent list.sort() / sorted()
        # comparing actual callable objects if their primary prio is the same
        secondary = next(cnt)
        return (primary, secondary, callobj)

    def run(self, pool=None):
        if not pool:
            pool = ThreadWorkerPool()
        counter = iter(range(len(self.tasks)))
        frontline = list((self._annotate_prio(t, counter)
                            for t in self.tasks if not get_requires(t)))
        allshared = dict(((task, self.default_shared.copy()) for task in self.tasks))

        frontline.sort()
        while frontline and not pool.full():
            _, _, task = frontline.pop(0)  # 3x faster than set with sorted()
            shared = allshared[task]
            pool.submit(task, shared)

        for resinfo in pool.iter_results():
            yield resinfo

            # tasks to be scheduled next, unblocked by fresh provides
            task, _, _ = resinfo
            next_tasks, children = self._satisfy_provides(self.deps, task)

            # propagate shared state from current to next tasks
            for child in children:
                allshared[child].update(allshared[task])
            del allshared[task]

            # put new tasks on the frontline
            next_with_prio = (self._annotate_prio(t, counter) for t in next_tasks)
            frontline.extend(next_with_prio)

            # and schedule as much as we can
            frontline.sort()
            while frontline and not pool.full():
                _, _, task = frontline.pop(0)
                shared = allshared[task]
                pool.submit(task, shared)

            if not frontline and not self.deps:
                pool.shutdown()
