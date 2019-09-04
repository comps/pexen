from collections import namedtuple

from ..util import PexenError

class SchedulerError(PexenError):
    pass

class TaskFailError(Exception):
    pass

# task result from pool/planner returned to the caller
TaskRes = namedtuple('TaskRes', ['task', 'shared', 'ret', 'excinfo'])
# TODO: use Python 3.7 namedtuple defaults
TaskRes.__new__.__defaults__ = ({}, None, None)  # from the right

# exception details, incl. traceback
ExceptionInfo = namedtuple('ExceptionInfo', ['type', 'val', 'tb'])
ExceptionInfo.__new__.__defaults__ = (None,) * len(ExceptionInfo._fields)
