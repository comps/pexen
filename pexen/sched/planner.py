"""
The scheduler takes callables and runs them according to their associated
metadata (attributes), as provided by pexen.sched.meta.

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

import warnings

# No. as_completed() or wait() don't return results as soon as they finish,
# they just iterate the list of futures from front to back, .. use a real pool
#from concurrent import futures

from . import meta, pool, common
from .. import util

class DepcheckError(common.SchedulerError):
    """Raised by the scheduler when a requires/provides check fails.

    The arg contains the remaining unsatisfied dependency metadata.
    """
    def __init__(self, msg, remain):
        self.remain = remain
        super().__init__(msg)

class MutexError(common.SchedulerError):
    """Raised by the scheduler when a mutex sanity check fails.

    The arg holds the task object that failed the check.
    """
    def __init__(self, msg, task):
        self.task = task
        super().__init__(msg)

class DepMap:
    """Used for tracking requires/provides."""
    def __init__(self, tasks):
        # indexed by dep name, each member having a (sub-)dict indexed by task
        # callable, each of which points to a set of requires of the task
        self._build_map(tasks)

    def __len__(self):
        return len(self.map)

    def _build_map(self, tasks):
        """Given a list of tasks, build the dependency map."""
        self.map = {}
        for task in tasks:
            reqs = meta.get_requires(task).copy()
            for req in reqs:
                if req in self.map:
                    self.map[req][task] = reqs
                else:
                    self.map[req] = {task: reqs}

    def _valid_provide(self, task, provide):
        """Sanity check if a provide is known/valid."""
        if provide not in self.map:
            warnings.warn(f"Dep '{provide}' provided by {task}, "
                          "but not required by any task",
                          category=util.PexenWarning)
            return False
        return True

    def children(self, task):
        """Return children tasks of a task, per requires/provides relations."""
        for provide in meta.get_provides(task):
            if self._valid_provide(task, provide):
                yield from self.map[provide].keys()

    def satisfy(self, task):
        """Go through a task's provides and satisfy all requires in the map.

        Return tasks ready to be run (all requires satisfied).
        """
        for provide in meta.get_provides(task):
            if self._valid_provide(task, provide):
                ctasks = self.map.pop(provide)
                for ctask, creqs in ctasks.items():
                    creqs.remove(provide)
                    if not creqs:
                        yield ctask

    def simulate(self, tasks):
        """Simulate dependency resolution to identify unmet requires.

        This action destroys/clears the DepMap instance!
        """
        frontline = list((t for t in tasks if not meta.get_requires(t)))
        for task in frontline:
            # imagine we run the task <here>
            nexttasks = self.satisfy(task)
            frontline.extend(nexttasks)
        if self.map:
            raise DepcheckError(
                f"Unsatisfied requires remain: {list(self.map)}", self.map)

class SharedMap:
    """Holds shared state for tasks."""
    def __init__(self, tasks, initstate={}):
        self._build_map(tasks, initstate)

    def __getitem__(self, key):
        return self.map[key]

    def __delitem__(self, key):
        del self.map[key]

    def _build_map(self, tasks, initstate):
        """Given a list of tasks, create a shared store for each."""
        self.map = dict(((task, initstate.copy()) for task in tasks))

    def update(self, tasks, data):
        """Update shared state of each task with data."""
        for task in tasks:
            if task in self.map:
                self.map[task].update(data)

class PriorityQueue:
    """Store tasks like a heapq, but using priority from sched meta."""
    def __init__(self, tasks=[]):
        self.counter = 0
        self.queue = []
        if tasks:
            self.add(tasks)

    def __len__(self):
        return len(self.queue)

    def __iter__(self):
        for _, _, task in self.queue:
            yield task

    def _gen_unique_id(self):
        self.counter += 1
        return self.counter

    def add(self, tasks):
        """Add new tasks to the queue and sort it."""
        for task in tasks:
            prio = meta.get_priority(task)
            # give each callobj a unique integer, to prevent list.sort()
            # comparing actual task objects if their primary prio is the same
            secprio = self._gen_unique_id()
            self.queue.append((prio, secprio, task))
        self.queue.sort()

    def pop(self):
        """Pop one most important task."""
        # sorted() on a set is slower, self.queue[1:] is also slower
        _, _, task = self.queue.pop(0)
        return task

    @classmethod
    def onesort(self, tasks):
        """Perform a one-time in-place sort on a list of tasks, using the
        sorting logic of this class, without creating a class instance.
        """
        q = self(tasks)
        tasks[:] = list(q)

class MutexMap:
    """Keeps track of claims/uses a.k.a. read-write locks by tasks.

    Works by holding onto tasks in an internal buffer and releasing them
    when they successfully lock the required resources.
    """
    def __init__(self, tasks=[]):
        # held tasks, ordered by priority
        self.buffer = []
        # indexed by lock
        self.rolocks = {}  # points to sets of tasks
        self.rwlocks = {}  # points to one task object
        # if tasks are provided, run sanity on them
        self.sanity_check(tasks)

    def __len__(self):
        return len(self.rolocks) + len(self.rwlocks)

    @staticmethod
    def sanity_check(tasks):
        for task in tasks:
            uses = meta.get_uses(task)
            claims = meta.get_claims(task)
            both = uses.intersection(claims)
            if both:
                raise MutexError(
                    f"Task {task} has {both} in both claims and uses.",
                    task)

    def _test_locks(self, task):
        """Test whether all locks a task requires can be acquired."""
        # rw and ro logic
        for use in meta.get_uses(task):
            # already held rw, cannot lock ro
            if use in self.rwlocks:
                return False
            # if it is held ro or not held at all, we can lock
        for claim in meta.get_claims(task):
            # already held rw/ro, cannot lock rw
            if claim in self.rwlocks or claim in self.rolocks:
                return False
        return True

    def _acquire_locks(self, task):
        if not self._test_locks(task):
            return False
        for use in meta.get_uses(task):
            if use in self.rolocks:
                self.rolocks[use].add(task)
            else:
                self.rolocks[use] = {task}
        for claim in meta.get_claims(task):
            self.rwlocks[claim] = task
        return True

    def _release_locks(self, task):
        for use in meta.get_uses(task):
            self.rolocks[use].remove(task)
            if not self.rolocks[use]:
                del self.rolocks[use]
        for claim in meta.get_claims(task):
            del self.rwlocks[claim]

    def add(self, tasks):
        """Filter a list of tasks through the locking logic.

        Return back tasks without locking needs or tasks that acquired all
        locks successfully.
        """
        waiting = []
        self.buffer.extend(tasks)
        PriorityQueue.onesort(self.buffer)
        for task in self.buffer:
            if meta.get_uses(task) or meta.get_claims(task):
                if self._acquire_locks(task):
                    yield task
                else:
                    waiting.append(task)
            else:
                yield task
        self.buffer = waiting

    def release(self, task):
        """Release any locks held by a task."""
        self._release_locks(task)

class Sched:
    def __init__(self, tasks=set()):
        # all added callables
        self.tasks = set()
        # default for shared state between chained tasks
        self.default_shared = {}
        if tasks:
            self.add_tasks(tasks)

    def add_tasks(self, new):
        self.tasks.update(set(new))
        self.depmap = DepMap(self.tasks)
        if __debug__:
            MutexMap.sanity_check(self.tasks)
            self.depmap.simulate(self.tasks)
            self.depmap = DepMap(self.tasks)

    def add_shared(self, **kwargs):
        """Pre-set key/values in the default shared space."""
        self.default_shared.update(kwargs)

    @staticmethod
    def _is_controlled_fail(excinfo):
        """Return True if the task exception is a user-induced one,
        as opposed to an unexpected one.
        """
        extype, exval, extb = excinfo
        return issubclass(extype, common.TaskFailError)

    # TODO: prevent run() from being called while tasks are running

    # TODO: does Sched instance support restarting by running run()
    #       again after it finishes? .. do we need to add_tasks()?
    #       --> document it

    def run(self, poolinst=None):
        if not self.tasks:
            return

        self.lockmap = MutexMap(self.tasks)
        self.sharedmap = SharedMap(self.tasks, initstate=self.default_shared)

        if not poolinst:
            poolinst = pool.ThreadWorkerPool()

        poolinst.register(self.tasks)
        poolinst.start()

        # initial frontline - tasks without deps and without locks
        # (or with successfully acquired locks)
        frontline = (t for t in self.tasks if not meta.get_requires(t))
        frontline = self.lockmap.add(frontline)
        frontline = PriorityQueue(frontline)

        while frontline and not poolinst.full():
            task = frontline.pop()
            shared = self.sharedmap[task]
            poolinst.submit(task, shared)

        for taskres in poolinst.iter_results():
            yield taskres

            # if it didn't fail or failed controllably
            if not taskres.excinfo or self._is_controlled_fail(taskres.excinfo):
                # process claims/uses, free mutexes since the task ended
                self.lockmap.release(taskres.task)

                # propagate shared state from current to next tasks
                children = self.depmap.children(taskres.task)
                self.sharedmap.update(children, taskres.shared)

                # tasks to be scheduled next, unblocked by fresh provides
                next_tasks = self.depmap.satisfy(taskres.task)

                # pass through only tasks that don't need any locks or that can
                # successfully acquire all of their locks
                next_tasks = self.lockmap.add(next_tasks)

                frontline.add(next_tasks)

            del self.sharedmap[taskres.task]

            # and schedule as much as we can
            while frontline and not poolinst.full():
                task = frontline.pop()
                shared = self.sharedmap[task]
                poolinst.submit(task, shared)

            # no new tasks to schedule, just cycle here in pool.iter_results()
            if not frontline and poolinst.empty():
                poolinst.shutdown()

        # there may still be unsatisfied deps/mutexes if a task failed
        deps = len(self.depmap)
        if deps > 0:
            warnings.warn(f"{deps} tasks skipped due to unmet deps "
                          "caused by parent exception",
                          category=util.PexenWarning)
        locks = len(self.lockmap)
        if locks > 0:
            warnings.warn(f"{locks} locks still held at exit due to "
                          "parent exception",
                          category=util.PexenWarning)

    # TODO: shutdown func in case user wants to abort; expose pool shutdown()?
