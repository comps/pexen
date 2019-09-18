import inspect
from fnmatch import fnmatchcase

from ..sched import meta
from .base import BaseFactory

class ModuleFactory(BaseFactory):
    """
    Takes an imported module object and extracts callable objects from it.

    A valid callable is any object that can be called and has pexen.sched
    metadata.

    Arguments:
        match - also include metadata-less callables that fnmatch this string
    """
    def __init__(self, match=None):
        super().__init__()
        self.match = match

    def is_valid_callable(self, objname, obj):
        if not callable(obj):
            return False
        if meta.has_meta(obj):
            return True
        if self.match and fnmatchcase(objname, self.match):
            return True
        return False

    def extract_from_mod(self, mod):
        """Extract callables from an imported module."""
        for name, obj in inspect.getmembers(mod):
            if not self.is_valid_callable(name, obj):
                continue
            self.callpath_burn(obj, name)
            yield obj

    def __call__(self, mod):
        yield from self.extract_from_mod(mod)
