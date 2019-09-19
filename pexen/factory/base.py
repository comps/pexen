CALLPATH_ATTR_NAME = 'pexen_factory_callpath_'

def get_callpath(obj):
    return getattr(obj, CALLPATH_ATTR_NAME)

class BaseFactory:
    def __init__(self):
        self.callpath = []

    def __call__(self):
        pass

    def callpath_transfer(self, tofactory):
        tofactory.callpath = self.callpath

    def callpath_push(self, name):
        self.callpath.append(name)

    def callpath_pop(self):
        self.callpath.pop()

    def callpath_burn(self, obj, objname='', path=[]):
        if not path:
            path = self.callpath
        if objname:
            path = path.copy()
            path.append(objname)
        setattr(obj, CALLPATH_ATTR_NAME, path)
