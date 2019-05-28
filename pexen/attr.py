#import dataclasses

ATTR_NAME = 'pexen_meta_'

class CallAttr:
    def __init__(self, **kwargs):
        object.__setattr__(self, 'data', {
            'callpath': [],
            'requires': set(),
            'provides': set(),
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

    def __setattr__(self, name, value):
        if name in self.data:
            field = self.data[name]
            self.data[name] = type(field)(value)
        else:
            raise AttributeError(f"{name} is not a valid metadata field")

    #def __getitem__(self, key):
    #    return self.__getattr__(key)
    #def __setitem__(self, key, value):
    #    self.__setattr__(key, value)

# dataclass cannot guarantee data type of its members unless completely frozen
#@dataclasses.dataclass
#class CallAttr:
#    callpath: list = dataclasses.field(default_factory=list)
#    requires: set = dataclasses.field(default_factory=set)
#    provides: set = dataclasses.field(default_factory=set)
#    claims: set = dataclasses.field(default_factory=set)

def assign_attr(callobj, meta=None):
    """Assign a metadata attribute to an object and return it.

    Useful if you create a CallAttr instance externally and want
    to assign it to an object.
    """
    if meta:
        setattr(callobj, ATTR_NAME, meta)
        return meta
    else:
        try:
            return getattr(callobj, ATTR_NAME)
        except AttributeError:
            new = CallAttr()
            setattr(callobj, ATTR_NAME, new)
            return new

def retrieve_attr(callobj):
    """Get a metadata attribute from an object."""
    return getattr(callobj, ATTR_NAME)

def assign_val(callobj, **kwargs):
    """Assign metadata values passed as kwargs to an object.

    Useful to easily set metadata fields directly without working
    with CallAttr.
    """
    try:
        meta = getattr(callobj, ATTR_NAME)
        #meta.__dict__.update(kwargs)  # dataclass
        meta.update(kwargs)
    except AttributeError:
        meta = CallAttr(**kwargs)
        setattr(callobj, ATTR_NAME, meta)

def _gen_get_subattr(name):
    def get_subattr(callobj):
        meta = assign_attr(callobj)
        #return meta.__dict__[name]   # dataclass
        return getattr(meta, name)
    return get_subattr

get_callpath = _gen_get_subattr('callpath')
get_requires = _gen_get_subattr('requires')
get_provides = _gen_get_subattr('provides')
get_claims   = _gen_get_subattr('claims')
get_priority = _gen_get_subattr('priority')
get_kwargs   = _gen_get_subattr('kwargs')

def func_attr(**kwargs):
    def decorate_func_attr(func):
        assign_attr(func, CallAttr(**kwargs))
        return func
    return decorate_func_attr
