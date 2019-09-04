#import dataclasses

SCHED_ATTR_NAME = 'pexen_sched_meta_'

class SchedMeta:
    def __init__(self, **kwargs):
        object.__setattr__(self, 'data', {
            'requires': set(),
            'provides': set(),
            'uses': set(),
            'claims': set(),
            'priority': 0,
            'kwargs': {},
        })
        self.update(kwargs)

    def update(self, keyvals):
        """Update attributes from a dict."""
        for key, val in keyvals.items():
            self.__setattr__(key, val)

    def __repr__(self):
        return repr(self.data)

    def __getattr__(self, name):
        if name in self.data:
            return self.data[name]
        else:
            object.__getattribute__(self, name)

    @staticmethod
    def _safe_cast(field, dtype, svalue):
        """Allow only some convenient type changes."""
        # any type to itself
        if dtype == type(svalue):
            return svalue
        # list -> set
        if issubclass(dtype, set) and isinstance(svalue, list):
            return dtype(svalue)
        # set -> list
        if issubclass(dtype, list) and isinstance(svalue, set):
            return dtype(svalue)
        raise AttributeError(f"{type(svalue)} cannot be used for {field}")

    def __setattr__(self, name, value):
        if name in self.data:
            field = self.data[name]
            self.data[name] = self._safe_cast(name, type(field), value)
        else:
            raise AttributeError(f"{name} is not a valid metadata field")

    #def __getitem__(self, key):
    #    return self.__getattr__(key)
    #def __setitem__(self, key, value):
    #    self.__setattr__(key, value)

# dataclass cannot guarantee data type of its members unless completely frozen
#@dataclasses.dataclass
#class SchedMeta:
#    requires: set = dataclasses.field(default_factory=set)
#    provides: set = dataclasses.field(default_factory=set)
#    claims: set = dataclasses.field(default_factory=set)

def assign_meta(callobj, meta=None):
    """Assign a metadata instance to an object and return it.

    If no instance is passed, an empty one is created.

    Useful if you create a SchedMeta instance externally and want
    to assign it to an object.
    """
    if meta:
        setattr(callobj, SCHED_ATTR_NAME, meta)
        return meta
    else:
        try:
            return getattr(callobj, SCHED_ATTR_NAME)
        except AttributeError:
            new = SchedMeta()
            setattr(callobj, SCHED_ATTR_NAME, new)
            return new

def has_meta(callobj):
    """Return True if the object has valid metadata assigned."""
    return hasattr(callobj, SCHED_ATTR_NAME)

def retrieve_meta(callobj):
    """Get a metadata instance from an object."""
    return getattr(callobj, SCHED_ATTR_NAME)

def assign_val(callobj, **kwargs):
    """Assign metadata values passed as kwargs to an object.

    Useful to easily set metadata fields directly without working
    with SchedMeta.
    """
    try:
        meta = getattr(callobj, SCHED_ATTR_NAME)
        #meta.__dict__.update(kwargs)  # dataclass
        meta.update(kwargs)
    except AttributeError:
        meta = SchedMeta(**kwargs)
        setattr(callobj, SCHED_ATTR_NAME, meta)

def _gen_get_subattr(name):
    def get_subattr(callobj):
        meta = assign_meta(callobj)
        #return meta.__dict__[name]   # dataclass
        return getattr(meta, name)
    return get_subattr

get_requires = _gen_get_subattr('requires')
get_provides = _gen_get_subattr('provides')
get_uses     = _gen_get_subattr('uses')
get_claims   = _gen_get_subattr('claims')
get_priority = _gen_get_subattr('priority')
get_kwargs   = _gen_get_subattr('kwargs')

def func_meta(func=None, **kwargs):
    if not func:
        # called with kwargs
        def decorate_func_meta(func):
            assign_val(func, **kwargs)
            return func
        return decorate_func_meta
    else:
        # called as arg-less decorator
        assign_val(func)
        return func
