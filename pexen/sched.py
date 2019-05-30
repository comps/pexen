import sys
import warnings
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
    Queue = multiprocessing.Queue
    Worker = multiprocessing.Process

    def __init__(self, workers=1, spare=1):
        self.workers = set()
        self.taskq = self.Queue()
        self.resultq = self.Queue()
        self.active_tasks = 0
        self.max_tasks = workers+spare
        self.task_index = {}  # so we don't pickle task objects
        self.shutting_down = False
        self.start_pool(workers)

    @staticmethod
    def _worker_body(outqueue, inqueue):
        while True:
            taskinfo = inqueue.get()
            if not taskinfo:
                break
            try:
                taskid, task, shared = taskinfo
                kwargs = get_kwargs(task)
                ret = task(shared, **kwargs)
                outqueue.put((taskid, ret, None))
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
                outqueue.put((taskid, None, (extype, exval, extb)))

                # in case the above ever gets fixed:
                #try:
                #    outqueue.put((taskid, None, (extype, exval, extb)))
                #except TypeError:
                #    try:
                #        outqueue.put((taskid, None, (extype, exval, None)))
                #    except TypeError:
                #        outqueue.put((taskid, None, (extype, None, None)))

    def start_pool(self, workers):
        for _ in range(workers):
            w = self.Worker(target=self._worker_body,
                            args=(self.resultq, self.taskq))
            w.start()
            self.workers.add(w)

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

    #def topup(self, taskinfos):
    #    itr = iter(taskinfos)
    #    cnt = self.max_tasks - self.active_tasks
    #    for _ in range(cnt):
    #        self.taskq.put(next(itr))
    #        self.active_tasks += 1
    #    return cnt

    def shutdown(self, wait=False):
        self.shutting_down = True
        for _ in range(len(self.workers)):
            self.taskq.put(None)
        if wait:
            for worker in self.workers:
                worker.join()
            # iter_results over queued results still valid after this

    def _cleanup_workers(self):
        if self.shutting_down and self.workers:
            # for multiprocessing, this does waitpid(2) and collects zombies
            for dead in [x for x in self.workers if not x.is_alive()]:
                self.workers.remove(dead)

    # if you need something asynchronous instead of the blocking iter_results,
    # ie. something returning a Queue, you may need to spawn a new thread to
    # manage the queue and call active_tasks -= 1 for each added item

    def iter_results(self):
        while True:
            if not self.workers and self.active_tasks <= 0:
                return
            self._cleanup_workers()
            if self.active_tasks > 0:
                taskid, res, exdata = self.resultq.get()
                task = self.task_index[taskid]
                self.active_tasks -= 1
                yield (task, res, exdata)

class ThreadWorkerPool(ProcessWorkerPool):
    Queue = queue.Queue
    Worker = threading.Thread

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

    def run(self):
        # tasks without requires
        frontline = list((t for t in self.tasks if not get_requires(t)))
        allshared = dict(((task, self.default_shared.copy()) for task in self.tasks))
        # TODO: concurrency futures
        for task in frontline:
            shared = allshared[task]
            kwargs = get_kwargs(task)
            #
            yield task(shared, **kwargs)
            #
            nexttasks, children = self._satisfy_provides(self.deps, task)
            for child in children:
                allshared[child].update(allshared[task])
            del allshared[task]
            frontline.extend(nexttasks)

#    @staticmethod
#    def _worker_wrap(inqueue)   # TODO: how to cleanly shut down idle workers? .. using an event?
#
#    def _manager(self, results, workers, executor, spare):
#        """Schedules workers, distributes tasks."""
#        # tasks without requires
#        frontline = list(((get_priority(t), t) for t in self.tasks if not get_requires(t)))  # TODO: add priority, make tuples
#        allshared = dict(((task, self.default_shared.copy()) for task in self.tasks))
#        ex = executor(max_workers=workers)
#
#        # TODO: below is fucked, rewrite using a pool with input queue, using "spare" cnt as max queue size,
#        #       and output queue, for result collection ...
#        #       ... run the entire child in a custom func that catches all exceptions and puts the result
#        #           on the queue as (result, exception)
#        #           ... then, in _manager, read this queue, re-wrap it as (task,result,exception) and put
#        #               it on the real result queue (always non-multiprocessing queue.Queue)
#        #
#        #       also you probably need to count how many results you got (same as nr. of tasks sent),
#        #       to know when to end, otherwise you might wait for a result from a pool of idle workers
#
#
#        #      add_done_callback(fn)
#
#        running = set()
#        running_max = workers+spare
#        while True:
#            # fill up running tasks
#            # we need to do this every cycle as there will be times where deps
#            # will restrict running tasks to less than running_max
#            frontline.sort()
#            while len(frontline) > 0 and len(running) < running_max:
#                _, task = fronline.pop(0)
#                shared = allshared[task]
#                kwargs = get_kwargs(task)
#                running.add(ex.submit(task, shared, **kwargs))
#            if not running:
#                return  # no tasks added, all done
#            finished = set()
#            for f in futures.as_completed(running):
#                results.put((
#                finished.add(f)
#
#            running.difference_update(finished)
#
#            #for i in range(max(0, running_max-len(running))):
#            #    running.add(ex.submit
#
#    # TODO: rename to run()
#    def run_thread(self, workers=1, executor=futures.ThreadPoolExecutor, spare=3):
#        """Returns a Queue with tuples of
#        (<callable>,<retval>,<exception>,<shared>)."""
#        results = queue.Queue()
#        t = threading.Thread(target=self._manager,
#                             args=(results, workers, executor, spare))
#        t.start()
#        self.manager_thread = t
#        return results
#
#    def finished(self):
#        return not self.manager_thread.is_alive()
#
#    # TODO: some check_error() function to see if manager raised an exception
#
#    def run_iter(self, *args, **kwargs):
#        results = self.run_thread(*args, **kwargs)
#        while True:
#            # TODO: check if manager raised an exception + re-raise it here
#            yield results.get()
#            if self.finished():
#                break
#
#
#    # TODO:
#    # - run() returns generator that needs to be regularly called to schedule next / get a result
#    # - run_bg() returns a Queue object (internally spawns a thread that does run() and puts results into the queue)
#
#    def shutdown(self):
#        # TODO: just prevent any new tasks from being scheduled and make
#        #       _manager return (is_alive() == False) as soon as all results
#        #       from currently running workers are pushed to the queue
#        #       ... also call executor.shutdown()
