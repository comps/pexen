import pkgutil
import inspect
import importlib
from . import attr


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

def _extract_callables(mod, callpath, filterby):
    for name, obj in inspect.getmembers(mod):
        if not callable(obj):
            continue
        if hasattr(mod, '__all__') and name not in mod.__all__:
            continue
        if filterby and name not in filterby:
            continue
        attr.assign_val(obj, callpath=callpath+[name])
        yield obj

def module_factory(*args, callpath=[], startfrom, filterby=[], ffunc=None,
                   **kwargs):
    """Traverse a (sub-)module hierarchy, from startfrom, collect callables.

    If filterby is defined as a list of strings, collect only callables matching
    (by name) at least one of the strings.

    If a package is encountered, recurse into it.
    If the package defines a callable named ffunc, immediately stop and call it
    instead of collecting callables. The ffunc callable is passed all arguments
    and a return value of a list of eligible callables is expected.
    """

    if not callpath:
        callpath = [startfrom.__name__]

    # the startfrom package itself; if it has ffunc, run it instead of us
    if ffunc:
        ffunc_obj = getattr(startfrom, ffunc, None)
        if callable(ffunc_obj):
            return ffunc_obj(args, callpath=callpath, startfrom=startfrom,
                             filterby=filterby, ffunc=ffunc, **kwargs)

    callables = []

    # callables from startfrom
    callables.extend(_extract_callables(startfrom, callpath, filterby))

    # callables from startfrom's submodules
    for filefinder, modname, ispkg in pkgutil.iter_modules(startfrom.__path__):
        mod = _import_from_path(filefinder, modname)
        if ispkg:
            new = module_factory(args, callpath=callpath+[modname],
                                 startfrom=mod, filterby=filterby,
                                 ffunc=ffunc, **kwargs)
            if (isinstance(new, list)):
                callables.extend(new)
        else:
            callables.extend(_extract_callables(mod, callpath, filterby))

    return callables
