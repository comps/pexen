from collections import namedtuple

from ..util import PexenError

class SchedulerError(PexenError):
    pass

class TaskFailError(Exception):
    def __init__(self, *args, deps_ok=True, locks_ok=True):
        self.deps_ok = deps_ok
        self.locks_ok = locks_ok
        super().__init__(*args)
#    def __str__(self):
#        return f'deps_ok: {self.deps_ok}, locks_ok: {self.locks_ok}'
#    def __repr__(self):
#        return f'{self.__class__.__name__}({self.__str__()})'

# task result from pool/planner returned to the caller
TaskRes = namedtuple('TaskRes', ['task', 'shared', 'ret', 'excinfo'])
# TODO: use Python 3.7 namedtuple defaults
TaskRes.__new__.__defaults__ = ({}, None, None)  # from the right

# exception details, incl. traceback
ExceptionInfo = namedtuple('ExceptionInfo', ['type', 'val', 'tb'])
ExceptionInfo.__new__.__defaults__ = (None,) * len(ExceptionInfo._fields)
