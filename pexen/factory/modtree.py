import importlib
import inspect
import pkgutil

from . import base

class ModTreeFactory(base.BaseFactory):
    @staticmethod
    def _import_from_path(path, name):
        """Load a module by a filesystem path."""
        if isinstance(path, importlib.machinery.FileFinder):
            spec = path.find_spec(name)
        else:
            spec = importlib.machinery.PathFinder.find_spec(name, path)
        if not spec:
            return None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def _extract_callables(self, mod):
        """Extract callables from an imported module."""
        for name, obj in inspect.getmembers(mod):
            if not self.is_valid_callable(obj):
                continue
            self.callpath_burn(obj, name)
            self.callables.append(obj)

    def open(self, startmod):
        """Open/connect/init a source of callables."""
        self.startmod = startmod

    def _walk_one(self, startfrom):
        self._extract_callables(startfrom)
        for filefinder, modname, ispkg in pkgutil.iter_modules(startfrom.__path__):
            mod = self._import_from_path(filefinder, modname)
            self.callpath_push(modname)
            if ispkg:
                self._walk_one(mod)
            else:
                self._extract_callables(mod)
            self.callpath_pop()

    def walk(self):
        """Recursively retrieve valid callables into self.callables."""
        self._walk_one(self.startmod)
        return self.callables
