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

import warnings

# No. as_completed() or wait() don't return results as soon as they finish,
# they just iterate the list of futures from front to back, .. use a real pool
#from concurrent import futures

from . import pool
from .attr import get_requires, get_provides, get_priority
from .util import SchedulerError
from .. import util

class DepcheckError(SchedulerError):
    """Raised by the scheduler when a requires/provides check fails.

    The arg contains the remaining unsatisfied dependency metadata.
    """
    def __init__(self, msg, remain):
        self.remain = remain
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
            reqs = get_requires(task).copy()
            for req in reqs:
                if req in self.map:
                    self.map[req][task] = reqs
                else:
                    self.map[req] = {task: reqs}

    def _valid_provide(self, provide):
        """Sanity check if a provide is known/valid."""
        if provide not in self.map:
            warnings.warn(f"Dep \"{provide}\" provided by {task}, "
                          "but not required by any task",
                          category=util.PexenWarning)
            return False
        return True

    def children(self, task):
        """Return children tasks of a task, per requires/provides relations."""
        for provide in get_provides(task):
            if self._valid_provide(provide):
                yield from self.map[provide].keys()

    def satisfy(self, task):
        """Go through a task's provides and satisfy all requires in the map.

        Returns tasks ready to be run (all requires satisfied).
        """
        for provide in get_provides(task):
            if self._valid_provide(provide):
                ctasks = self.map.pop(provide)
                for ctask, creqs in ctasks.items():
                    creqs.remove(provide)
                    if not creqs:
                        yield ctask

    def simulate(self, tasks):
        """Simulate dependency resolution to identify unmet requires.

        This action destroys/clears the DepMap instance!"""
        frontline = list((t for t in tasks if not get_requires(t)))
        for task in frontline:
            # imagine we run the task <here>
            nexttasks = self.satisfy(task)
            frontline.extend(nexttasks)
        if self.map:
            raise DepcheckError(
                f"Unsatisfied requires remain: {list(self.map.keys())}",
                self.map)

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

# TODO: "claims" functionality/attr

# TODO: task groups / exclusivity (based on pool restarting)

class Sched:
    def __init__(self, tasks=set()):
        # all added callables
        self.tasks = set()
        # default for shared state between chained tasks
        self.default_shared = {}
        # whether the current task set was sanity checked
        self.checked = False
        if tasks:
            self.add_tasks(tasks)

    def add_tasks(self, new):
        self.tasks.update(set(new))
        self.depmap = DepMap(self.tasks)
        if __debug__:
            self.depmap.simulate(self.tasks)
            self.depmap = DepMap(self.tasks)

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

    def run(self, pooltype=pool.ThreadWorkerPool, workers=1, spare=1):
        if not self.tasks:
            return

        counter = iter(range(len(self.tasks)))
        frontline = list((self._annotate_prio(t, counter)
                         for t in self.tasks if not get_requires(t)))
        self.sharedmap = SharedMap(self.tasks, initstate=self.default_shared)
        pool = pooltype(alltasks=self.tasks, workers=workers, spare=spare)

        frontline.sort()
        while frontline and not pool.full():
            _, _, task = frontline.pop(0)  # 3x faster than set with sorted()
            shared = self.sharedmap[task]
            pool.submit(task, shared)

        for taskres in pool.iter_results():
            yield taskres

            # if it didn't fail, schedule its children
            if not taskres.excinfo:
                # propagate shared state from current to next tasks
                children = self.depmap.children(taskres.task)
                self.sharedmap.update(children, taskres.shared)

                # tasks to be scheduled next, unblocked by fresh provides
                next_tasks = self.depmap.satisfy(taskres.task)

                # put new tasks on the frontline
                next_with_prio = (self._annotate_prio(t, counter) for t in next_tasks)
                frontline.extend(next_with_prio)
                frontline.sort()

            del self.sharedmap[taskres.task]

            # and schedule as much as we can
            while frontline and not pool.full():
                _, _, task = frontline.pop(0)
                shared = self.sharedmap[task]
                pool.submit(task, shared)

            # there may still be unsatisfied deps if a task failed
            if not frontline:
                remain = len(self.depmap)
                if remain > 0:
                    warnings.warn(f"{remain} tasks skipped due to unmet deps "
                                  "caused by parent exception",
                                  category=util.PexenWarning)
                pool.shutdown()

    # TODO: shutdown func in case user wants to abort; expose pool shutdown()?
