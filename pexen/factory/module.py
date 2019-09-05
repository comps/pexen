import inspect
from fnmatch import fnmatchcase

from ..sched import meta
from .base import BaseFactory

class ModuleFactory(BaseFactory):
    def __init__(self, match=None):
        super().__init__()
        self.match = match

    def is_valid_callable(self, objname, obj):
        if callable(obj) and meta.has_meta(obj):
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
