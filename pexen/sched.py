import warnings
#from concurrent import futures

from .attr import get_requires, get_provides, get_kwargs
from . import util

class Sched:
    def __init__(self, tasks=set()):
        # all added callables
        self.tasks = set()
        # scheduled callables for immediate execution
        self.frontline = []
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
            if not provide in alldeps:
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
            ret = task(shared, **kwargs)
            #
            nexttasks, children = self._satisfy_provides(self.deps, task)
            for child in children:
                allshared[child].update(allshared[task])
            del allshared[task]
            frontline.extend(nexttasks)
