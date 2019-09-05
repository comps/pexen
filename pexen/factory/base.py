from ..sched import meta

CALLPATH_ATTR_NAME = 'pexen_factory_callpath_'

def get_callpath(obj):
    return getattr(obj, CALLPATH_ATTR_NAME)

class BaseFactory:
    def __init__(self):
        self.callpath = []

    def __call__(self):
        pass

    @staticmethod
    def is_valid_callable(obj):
        if callable(obj) and meta.has_meta(obj):
            return True
        return False

    def callpath_start(self, startfrom):
        self.callpath = startfrom.copy()

    def callpath_push(self, name):
        self.callpath.append(name)

    def callpath_pop(self):
        self.callpath.pop()

    def callpath_burn(self, obj, objname=''):
        path = self.callpath.copy()
        if objname:
            path.append(objname)
        setattr(obj, CALLPATH_ATTR_NAME, path)
