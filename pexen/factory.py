import pkgutil
import inspect
import importlib

from . import attr

FACTORY_FLAG = 'pexen_is_factory'

def is_factory(func):
    """Decorator for marking arbitrary functions as factories."""
    setattr(func, FACTORY_FLAG, True)
    return func

def is_no_longer_factory(func):
    """Function of un-marking arbitrary functions as factories."""
    delattr(func, FACTORY_FLAG)

def _import_from_path(path, name):
    if isinstance(path, importlib.machinery.FileFinder):
        spec = path.find_spec(name)
    else:
        spec = importlib.machinery.PathFinder.find_spec(name, path)
    if not spec:
        return None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def _get_subfactory(mod):
    for name, obj in inspect.getmembers(mod):
        if callable(obj) and hasattr(obj, FACTORY_FLAG):
            return obj
    return None

def _is_valid_callable(obj):
    if callable(obj) and attr.has_attr(obj):
        return True
    return False

def _extract_callables(mod, callpath):
    for name, obj in inspect.getmembers(mod):
        if not _is_valid_callable(obj):
            continue
        attr.assign_val(obj, callpath=callpath+[name])
        yield obj

def module_factory(*args, callpath=[], startfrom, **kwargs):
    """Traverse a (sub-)module hierarchy, from startfrom, collect callables.

    Only callables decorated by attr.func_attr() are included.

    If a package is encountered, recurse into it.

    If the package defines a callable decorated by is_factory, stop immediately
    and call it (instead of collecting callables), passing all arguments.
    It should return a list of callables (like this factory func).
    If multiple callables within one module are decorated by is_factory, one
    will be arbitrarily chosen and the others ignored.

    Any kwargs specified are set in attrs of returned callables, to be passed
    during execution time.
    """

    if not callpath:
        callpath = [startfrom.__name__]

    # the startfrom package itself; if it has custom factory, run it instead
    sub = _get_subfactory(startfrom)
    if sub:
        return sub(args, callpath=callpath, startfrom=startfrom, **kwargs)

    callables = []

    # callables from startfrom
    callables.extend(_extract_callables(startfrom, callpath))

    # callables from startfrom's submodules
    for filefinder, modname, ispkg in pkgutil.iter_modules(startfrom.__path__):
        mod = _import_from_path(filefinder, modname)
        if ispkg:
            new = module_factory(args, callpath=callpath+[modname],
                                 startfrom=mod, **kwargs)
#            if (isinstance(new, list)):
#                callables.extend(new)
            callables.extend(new)
        else:
            callables.extend(_extract_callables(mod, callpath))

    return callables

# TODO: fstree_factory - walking directories for executable files
