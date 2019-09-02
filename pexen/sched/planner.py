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

    def run(self, pooltype=pool.ThreadWorkerPool, workers=1, spare=1):
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

        for taskres in pool.iter_results():
            yield taskres

            # if it didn't fail, schedule its children
            if not taskres.excinfo:
                # tasks to be scheduled next, unblocked by fresh provides
                next_tasks, children = self._satisfy_provides(self.deps, taskres.task)

                # propagate shared state from current to next tasks
                for child in children:
                    allshared[child].update(taskres.shared)

                # put new tasks on the frontline
                next_with_prio = (self._annotate_prio(t, counter) for t in next_tasks)
                frontline.extend(next_with_prio)
                frontline.sort()

            del allshared[taskres.task]

            # and schedule as much as we can
            while frontline and not pool.full():
                _, _, task = frontline.pop(0)
                shared = allshared[task]
                pool.submit(task, shared)

            # there may still be unsatisfied self.deps if a task failed
            if not frontline:
                remain = len(self.deps)
                if remain > 0:
                    warnings.warn(f"{remain} tasks skipped due to unmet deps "
                                  "caused by parent exception",
                                  category=util.PexenWarning)
                pool.shutdown()

    # TODO: shutdown func in case user wants to abort; expose pool shutdown()?
