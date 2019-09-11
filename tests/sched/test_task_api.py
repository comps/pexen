#
# Task metadata/kwargs/shared/return API
#

import pytest
import multiprocessing as mp
import time
import warnings

from pexen import sched, util
from pexen.sched import TaskRes
#from pexen.sched.pool import ThreadWorkerPool, ProcessWorkerPool

from tests.sched.common import *
from tests.sched.common import check_resources

#
# Return from a task
#

@parametrize_pool_both()
def test_return_value(pool):
    dummy1 = create_dummy_with_return('dummy1', 1234)
    s = sched.Sched([dummy1])
    res = list(s.run(pool()))
    assert res == [TaskRes(dummy1, ret=1234)]

@parametrize_pool_both()
def test_return_exception(pool):
    dummy1 = create_dummy_with_exception('dummy1', NameError)
    s = sched.Sched([dummy1])
    res = list(s.run(pool()))
    dummy1res = res[0]
    assert dummy1res.task == dummy1
    assert dummy1res.excinfo.type == NameError
    assert isinstance(dummy1res.excinfo.val, NameError)

# other returned fields of TaskRes are tested below

#
# Priority
#

@parametrize_pool_both()
def test_priority(pool):
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, priority=3)
    dummy2 = create_dummy('dummy2')
    sched.meta.assign_val(dummy2, priority=4)
    s = sched.Sched([dummy2, dummy1])
    res = list(s.run(pool()))
    assert res == [TaskRes(dummy1), TaskRes(dummy2)]
    sched.meta.assign_val(dummy1, priority=4)
    sched.meta.assign_val(dummy2, priority=3)
    s = sched.Sched([dummy2, dummy1])
    res = list(s.run(pool()))
    assert res == [TaskRes(dummy2), TaskRes(dummy1)]

#
# Deps (requires/provides)
#

@parametrize_pool_both()
def test_requires_provides(pool):
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, requires=['dep'], priority=1)
    dummy2 = create_dummy('dummy2')
    sched.meta.assign_val(dummy2, provides=['dep'], priority=2)
    s = sched.Sched([dummy2, dummy1])
    res = list(s.run(pool()))
    assert res == [TaskRes(dummy2), TaskRes(dummy1)]

@parametrize_pool_both()
def test_wait_for_deps(pool):
    # when frontline gets exhausted while there are still some tasks
    # to run within deps references, the planner should not exit
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, provides=['one'])
    dummy2 = create_dummy('dummy2')
    sched.meta.assign_val(dummy2, requires=['one'], provides=['two'])
    dummy3 = create_dummy('dummy3')
    sched.meta.assign_val(dummy3, requires=['two'])
    s = sched.Sched([dummy1, dummy2, dummy3])
    res = list(s.run(pool(workers=4)))
    assert res == [TaskRes(dummy1), TaskRes(dummy2), TaskRes(dummy3)]

def test_required_not_provided():
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, requires=['one'])
    with pytest.raises(sched.DepcheckError) as exc:
        s = sched.Sched([dummy1])
    assert "Unsatisfied requires remain: ['one']" in str(exc.value)

# we cannot easily test a case of required_not_provided with __debug__ == False
# due to __debug__ not being configurable during runtime
# the code, however, should behave like in test_failed_deps

def test_provided_not_required():
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, provides=['one'])
    with warnings.catch_warnings(record=True) as caught:
        s = sched.Sched([dummy1])
    assert len(caught) == 1  # only one warning
    assert caught[0].category == util.PexenWarning
    assert "Dep 'one' provided by" in str(caught[0].message)
    assert "but not required by any task" in str(caught[0].message)

#
# Locks (uses/claims)
#

def test_lock_sanity():
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, uses=[1,2,3], claims=[4,2,3,6])
    with pytest.raises(sched.MutexError) as exc:
        s = sched.Sched([dummy1])
    assert "has {2, 3} in both claims and uses" in str(exc.value)

@parametrize_pool_both()
def test_uses(pool):
    q = mp.Queue()
    dummy1 = create_dummy_that_types('dummy1')
    sched.meta.assign_val(dummy1, uses=['res'], priority=1, kwargs={'q': q})
    dummy2 = create_dummy_that_types('dummy2')
    sched.meta.assign_val(dummy2, uses=['res'], priority=2, kwargs={'q': q})
    s = sched.Sched([dummy1, dummy2])
    res = list(s.run(pool(workers=1)))
    assert res == [TaskRes(dummy1), TaskRes(dummy2)]
    output = ''.join(map(lambda x: q.get(), range(q.qsize())))
    assert output == '1111122222'
    #s.add_tasks([dummy1, dummy2])
    s = sched.Sched([dummy1, dummy2])
    res = list(s.run(pool(workers=2)))
    assert TaskRes(dummy1) in res
    assert TaskRes(dummy2) in res
    output = ''.join(map(lambda x: str(q.get()), range(q.qsize())))
    assert '11111' not in output
    #assert '121' in output or '212' in output

@parametrize_pool_both()
def test_claims(pool):
    q = mp.Queue()
    dummy1 = create_dummy_that_types('dummy1')
    sched.meta.assign_val(dummy1, claims=['res'], priority=1, kwargs={'q': q})
    dummy2 = create_dummy_that_types('dummy2')
    sched.meta.assign_val(dummy2, claims=['res'], priority=2, kwargs={'q': q})
    s = sched.Sched([dummy1, dummy2])
    res = list(s.run(pool(workers=1)))
    assert res == [TaskRes(dummy1), TaskRes(dummy2)]
    output = ''.join(map(lambda x: q.get(), range(q.qsize())))
    assert output == '1111122222'
    #s.add_tasks([dummy1, dummy2])
    s = sched.Sched([dummy1, dummy2])
    res = list(s.run(pool(workers=2)))
    assert TaskRes(dummy1) in res
    assert TaskRes(dummy2) in res
    output = ''.join(map(lambda x: str(q.get()), range(q.qsize())))
    assert output == '1111122222'

# TODO: intentionally re-create priority inversion / higher-prio writer starvation

#
# Shared/kwargs interface
#

@parametrize_pool_both()
def test_shared(pool):
    dummy1 = create_dummy_with_shared('dummy1', 1234)
    sched.meta.assign_val(dummy1, requires=['dep'])
    dummy2 = create_dummy_with_shared('dummy2', 2345)
    sched.meta.assign_val(dummy2, provides=['dep'])
    s = sched.Sched([dummy2, dummy1])
    res = list(s.run(pool()))
    dummy2res, dummy1res = res  # order guaranteed by dep
    assert dummy2res == TaskRes(dummy2, shared={'dummy2': 2345})
    assert dummy1res == TaskRes(dummy1, shared={'dummy2': 2345, 'dummy1': 1234})

@parametrize_pool_both()
def test_preset_shared(pool):
    dummy1 = create_dummy_with_shared('dummy1', 1234)
    dummy2 = create_dummy_with_shared('dummy2', 2345)
    s = sched.Sched([dummy1, dummy2])
    s.add_shared(bothx=4444, dummy2=6666)
    s.add_shared(bothy=5555)
    res = list(s.run(pool()))
    assert TaskRes(dummy1, shared={'bothx': 4444, 'bothy': 5555,
                                   'dummy1': 1234, 'dummy2': 6666}) in res
    # pre-set 6666 was overwritten and propagated to parent
    assert TaskRes(dummy2, shared={'bothx': 4444, 'bothy': 5555,
                                   'dummy2': 2345}) in res

@parametrize_pool_both()
def test_kwargs(pool):
    dummy1 = create_dummy_with_kwargs('dummy1')
    args = {'testargs': 1234}
    sched.meta.assign_val(dummy1, kwargs=args)
    s = sched.Sched([dummy1])
    res = list(s.run(pool()))
    assert res == [TaskRes(dummy1, ret=args)]

@parametrize_pool_both()
def test_shared_kwargs(pool):
    dummy1 = create_dummy_shared_kwargs('dummy1', 1234)
    args = {'testargs': 2345}
    sched.meta.assign_val(dummy1, kwargs=args)
    s = sched.Sched([dummy1])
    res = list(s.run(pool()))
    assert res == [TaskRes(dummy1, shared={'dummy1': 1234}, ret=args)]

#
# Task failure and chaining (unmet deps, held locks)
#

@parametrize_pool_both()
def test_failed_deps(pool):
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, requires=['dep'])
    dummy2 = create_dummy_with_exception('dummy2', NameError)
    sched.meta.assign_val(dummy2, provides=['dep'])
    s = sched.Sched([dummy2, dummy1])
    with warnings.catch_warnings(record=True) as caught:
        res = list(s.run(pool()))
    assert len(res) == 1  # dummy1 didn't run
    dummy2res = res[0]
    assert dummy2res.task == dummy2
    assert dummy2res.excinfo.type == NameError
    assert isinstance(dummy2res.excinfo.val, NameError)
    assert len(caught) == 1  # only one warning
    assert caught[0].category == util.PexenWarning
    assert "1 tasks skipped due to unmet deps" in str(caught[0].message)

@parametrize_pool_both()
def test_failed_deps_controlled(pool):
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, requires=['dep'])
    dummy2 = create_dummy_with_exception('dummy2', sched.TaskFailError)
    sched.meta.assign_val(dummy2, provides=['dep'])
    s = sched.Sched([dummy2, dummy1])
    with warnings.catch_warnings(record=True) as caught:
        res = list(s.run(pool()))
    assert len(res) == 2  # dummy1 did run
    dummy2res, dummy1res = res
    assert dummy2res.task == dummy2
    assert dummy2res.excinfo.type == sched.TaskFailError
    assert isinstance(dummy2res.excinfo.val, sched.TaskFailError)
    assert dummy1res.task == dummy1
    assert len(caught) == 0  # no warnings

@parametrize_pool_both()
def test_failed_locks(pool):
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, priority=2, claims=['res'])
    dummy2 = create_dummy_with_exception('dummy2', NameError)
    sched.meta.assign_val(dummy2, priority=1, claims=['res'])
    s = sched.Sched([dummy1, dummy2])
    with warnings.catch_warnings(record=True) as caught:
        res = list(s.run(pool()))
    assert len(res) == 1  # dummy1 didn't run
    dummy2res = res[0]
    assert dummy2res.task == dummy2
    assert dummy2res.excinfo.type == NameError
    assert isinstance(dummy2res.excinfo.val, NameError)
    assert len(caught) == 1  # only one warning
    assert caught[0].category == util.PexenWarning
    assert "1 locks still held at exit" in str(caught[0].message)

@parametrize_pool_both()
def test_failed_locks_controlled(pool):
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, priority=2, claims=['res'])
    dummy2 = create_dummy_with_exception('dummy2', sched.TaskFailError)
    sched.meta.assign_val(dummy2, priority=1, claims=['res'])
    s = sched.Sched([dummy1, dummy2])
    with warnings.catch_warnings(record=True) as caught:
        res = list(s.run(pool()))
    assert len(res) == 2  # dummy1 did run
    dummy2res, dummy1res = res
    assert dummy2res.task == dummy2
    assert dummy2res.excinfo.type == sched.TaskFailError
    assert isinstance(dummy2res.excinfo.val, sched.TaskFailError)
    assert dummy1res.task == dummy1
    assert len(caught) == 0  # no warnings


# TODO: TaskRes.shared should be populated even if the task failed with uncontrollably with exception
#       (damaged state left to user, not propagated to other tasks)

# TODO: try .run() with circular deps; with __debug__ == False
#        - to test how planner behaves if initial frontline is empty

# TODO: try .run() with only one task which requires= something
#        - should fail on missing dep

# TODO: try .run() with only one tasks that rwlocks something and uncontrollably fails
#        - planner should return one TaskRes with exception and exit with one warning about 1 held lock

#
# Picklability checks
#

# Multiprocessing - unpicklable objects should generate an exception

@parametrize_pool_process()
def test_unpicklable_ret(pool):
    nonpickl = create_dummy('')
    dummy1 = create_dummy_with_return('dummy1', nonpickl)
    s = sched.Sched([dummy1])
    res = list(s.run(pool()))
    dummy1res = res[0]
    assert dummy1res.task == dummy1
    assert dummy1res.excinfo.type == AttributeError
    assert "Can't pickle" in str(dummy1res.excinfo.val)

@parametrize_pool_process()
def test_unpicklable_shared(pool):
    nonpickl = create_dummy('')
    dummy1 = create_dummy_with_shared('dummy1', nonpickl)
    s = sched.Sched([dummy1])
    res = list(s.run(pool()))
    dummy1res = res[0]
    assert dummy1res.task == dummy1
    assert dummy1res.excinfo.type == AttributeError
    assert "Can't pickle" in str(dummy1res.excinfo.val)

@parametrize_pool_process()
def test_unpicklable_ret_shared(pool):
    nonpickl = create_dummy('')
    dummy1 = create_dummy_with_shared('dummy1', nonpickl, ret=nonpickl)
    s = sched.Sched([dummy1])
    res = list(s.run(pool()))
    dummy1res = res[0]
    assert dummy1res.task == dummy1
    assert dummy1res.excinfo.type == AttributeError
    assert "Can't pickle" in str(dummy1res.excinfo.val)

# Threading - unpicklable objects should be passed without an issue

@parametrize_pool_thread()
def test_unpicklable_ret_shared_thread(pool):
    nonpickl = create_dummy('')
    dummy1 = create_dummy_with_shared('dummy1', nonpickl, ret=nonpickl)
    s = sched.Sched([dummy1])
    res = list(s.run(pool()))
    assert res == [TaskRes(dummy1, shared={'dummy1': nonpickl}, ret=nonpickl)]

#
# Corner cases
#

# worker passing user-provided kwargs to callable without kw args
@parametrize_pool_both()
def test_kwargs_without_args(pool):
    dummy1 = create_dummy('dummy1')
    args = {'testargs': 1234}
    sched.meta.assign_val(dummy1, kwargs=args)
    s = sched.Sched([dummy1])
    res = list(s.run(pool()))
    dummy1res = res[0]
    assert dummy1res.task == dummy1
    assert dummy1res.excinfo.type == TypeError
    assert "unexpected keyword argument" in str(dummy1res.excinfo.val)

# invalid data type being passed as meta
def test_invalid_meta_type():
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, requires=set('dep'))  # same type
    sched.meta.assign_val(dummy1, requires=['dep'])     # allowed conversion
    with pytest.raises(AttributeError) as exc:
        sched.meta.assign_val(dummy1, requires='dep')   # denied conversion
    assert "<class 'str'> cannot be used for requires" in str(exc.value)

