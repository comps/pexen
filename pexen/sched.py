"""
The scheduler takes callables and runs them according to their associated
metadata (attributes), as provided by pexen.attr.

The dependency resolution and scheduling algorithm itself is not deterministic
unless the callable execution time (and all the platform parallelism mechanisms)
are also deterministic.
Rather than splitting work across a set amount of workers, it dynamically
allocates callables (a.k.a. "tasks") to workers based on priority and worker
availability, optimizing the overall execution time as much as possible by
avoiding starvation.

Under the hood, the core of the algorithm can be described like this:
  1) Take any tasks that don't have any "requires" and put them on a "frontline"
     (a list of tasks not blocked by a dependency)
  2) Sort the frontline according to priority and/or other factors
  3) Schedule (run) tasks on the frontline
  4) As the tasks finish, collect their results and if they "provide" something,
     satisfy any other tasks' requires with it
  5) If this results in a task having all requires satisfied, add it to the
     frontline
  6) Go to 2

This, amongst other things, gives the user the ability to assign a high priority
to a long-running task, so that it's scheduled as soon as its dependencies are
cleared.

Usage:
  s = Sched([callable1, callable2])
  s.add_tasks([callable3])
  results = s.run()
  for res in results:
    print(res)
"""

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
    #       multiple iterators, each being thread-safe or process-safe, depending
    #       on the WorkerPool type
    #  TODO: the alive_workers / active_tasks is not thread safe; add a threading.lock
    def iter_results(self):
        while self.alive_workers > 0 or self.active_tasks > 0:
            msg = self.resultq.get()
            if msg.msgtype == 'finished':
                self.workers[msg.workid].join()
                self.alive_workers -= 1
            elif msg.msgtype == 'taskdone':
                task = self.task_map[msg.taskidx]
                self.active_tasks -= 1
                yield (task, msg.shared, msg.ret, msg.excinfo)
            else:
                raise RuntimeError(f"unexpected msg: {msg}")

class ThreadWorkerPool(ProcessWorkerPool):
    _Queue = queue.Queue
    _Worker = threading.Thread

# TODO: "claims" functionality/attr

# TODO: task groups / exclusivity (based on pool restarting)

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

    @staticmethod
    def _annotate_prio(callobj, cnt):
        primary = get_priority(callobj)
        # give each callobj a unique integer, to prevent list.sort() / sorted()
        # comparing actual callable objects if their primary prio is the same
        secondary = next(cnt)
        return (primary, secondary, callobj)

    def run(self, pooltype=ThreadWorkerPool, workers=1, spare=1):
        if not self.tasks:
            return

        counter = iter(range(len(self.tasks)))
        frontline = list((self._annotate_prio(t, counter)
                         for t in self.tasks if not get_requires(t)))
        allshared = dict(((task, self.default_shared.copy()) for task in self.tasks))
        pool = pooltype(alltasks=self.tasks, workers=workers, spare=spare)

        frontline.sort()
        while frontline and not pool.full():
            _, _, task = frontline.pop(0)  # 3x faster than set with sorted()
            shared = allshared[task]
            pool.submit(task, shared)

        for resinfo in pool.iter_results():
            yield resinfo

            task, retshared, _, _ = resinfo
            if retshared is None:
                retshared = allshared[task]  # last known good
            del allshared[task]

            # tasks to be scheduled next, unblocked by fresh provides
            next_tasks, children = self._satisfy_provides(self.deps, task)

            # propagate shared state from current to next tasks
            for child in children:
                allshared[child].update(retshared)

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

    # TODO: shutdown func in case user wants to abort; expose pool shutdown()?
