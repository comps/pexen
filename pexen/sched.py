import warnings

#from . import attr
from .attr import get_requires, get_provides
from . import util

class Sched:
    def __init__(self, tasks=set()):
        # all added callables
        self.tasks = set()
        # scheduled callables for immediate execution
        self.frontline = []
        # provided resources pointing to tasks requiring them
        self.deps = {}
        if tasks:
            self.add_tasks(tasks)
        print(self.deps)

    def add_tasks(self, new):
        self.tasks.update(set(new))
        self._build_deps(new, self.deps)

    def _build_deps(self, tasks, alldeps={}):
        """Build a dict of task dependencies (requires/provides).

        Top-level dict is indexed by dep name, each having a (sub-)dict indexed
        by task callable, each of which points to a set of other deps of the task.
        """
        for task in tasks:
            reqs = get_requires(task)
            for req in reqs:
                if req in alldeps:
                    alldeps[req][task] = reqs
                else:
                    alldeps[req] = {task: reqs}
        return alldeps


            #attr = attr.retrieve_attr(task)
            #if attr:
            # TODO exactly here:
            # - instead of using task obj directly, create a dict/list/tuple/whatever
            #   with a list of (all) the task's requires
            #   - so the list can be modified during simulation/runtime without altering
            #     the object attrs
            #

#            for dep in get_requires(task):
#                if dep in self.deps:
#                    self.deps[dep].add(task)
#                else:
#                    self.deps[dep] = set(task)

#    def __go_deeper(self, 

#    def _check_satisfied(self, pedantic=True):
#        """Check if all requires can be satisfied by provides."""
#        pass
#        #for task in self.tasks:
#        # TODO: we can't simply check if any provide satisfies any require,
#        # the tasks might be ordered in a way where a provide is never provided
#        # because the task won't run due to unsatisfied require
#        # - we need to actually schedule and simulate a (single-threaded) run
#
#    def _check_circular(self, deps, maxdepth=10, chain=[]):
#        """Check for circular dependencies up to the specified depth."""
#        for dep in deps:
#            tasks = self.deps[dep]
#            for task in tasks:
#                attr = attr.retrieve_attr(task)
#                if attr:
#                    for subdep in attr.provides

    @staticmethod
    def _satisfy_provide(alldeps, provide, task):
        """Returns a set of tasks ready to be run."""
        torun = set()
        if not provide in alldeps:
            warnings.warn(f"dep \"{provide}\" provided by {task}, "
                          "but not required by any task",
                          category=util.PexenWarning)
            return torun
        tasks = alldeps.pop(provide)
        for task, reqs in tasks.items():
            reqs.remove(provide)
            if not reqs:
                torun.add(task)
        return torun

    def _simulate_deps(self):
        alldeps = self._build_deps(self.tasks)
        frontline = list((t for t in self.tasks if not get_requires(t)))
        for task in frontline:
            print(f"frontline: {frontline}")
            # imagine we run the task <here>
            # now satisfy deps by this task's provides
            for prov in get_provides(task):
                frontline.extend(self._satisfy_provide(alldeps, prov, task))
        if alldeps:
            print("alldeps NOT empty!:")
            print(alldeps)

        # extract all leaf nodes from self.tasks into self.frontline
        # make a copy/backup of self.deps (as it will be modified)
        # "run" them one by one
        # - satisfy what they provide by resolving/removing items from self.deps
        #   - adding more tasks to self.frontline
        # when frontline is empty:
        # - if there are still deps left in self.deps, they were not satisfied
        #   - and tasks they point to were not run
        #     - this automagically includes circular deps!!
        #
        # run simulation (and other checks) automatically only if __debug__ is true
        # - allow the user to run them manually


#    def schedule(self, runchecks=True):
#        if runchecks:
#            self._check_satisfied()
#            self._check_circular(self.deps.keys())
