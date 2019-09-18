import importlib
import inspect
import pkgutil

from .base import BaseFactory
from .module import ModuleFactory

class ModTreeFactory(BaseFactory):
    """
    Walks a tree of modules/packages, calls ModuleFactory on each.

    Note that any modules found are imported and all callables they contain
    after import are considered (as per ModuleFactory rules).
    This means you can selectively alter the callables available via import-time
    execution.

    Arguments:
        match - passed to ModuleFactory as-is
    """
    def __init__(self, match=None):
        super().__init__()
        self.match = match

    @staticmethod
    def import_from_path(path, name):
        """Load a module by a filesystem path."""
        if isinstance(path, importlib.machinery.FileFinder):
            spec = path.find_spec(name)
        else:
            spec = importlib.machinery.PathFinder.find_spec(name, path)
        if not spec:
            raise ImportError(f"Could not find spec for {name}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def _extract_from_mod(self, mod):
        subf = ModuleFactory(match=self.match)
        self.callpath_transfer(subf)
        yield from subf(mod)

    def _walk_one(self, startfrom):
        yield from self._extract_from_mod(startfrom)
        for filefinder, modname, ispkg in pkgutil.iter_modules(startfrom.__path__):
            mod = self.import_from_path(filefinder, modname)
            self.callpath_push(modname)
            if ispkg:
                yield from self._walk_one(mod)
            else:
                yield from self._extract_from_mod(mod)
            self.callpath_pop()

    def __call__(self, startmod):
        yield from self._walk_one(startmod)
