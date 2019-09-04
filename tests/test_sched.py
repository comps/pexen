import pytest
import sys
import functools
import threading as thr
import multiprocessing as mp
import time
import psutil
import warnings

from pexen import sched, util
from pexen.sched import TaskRes
from pexen.sched.pool import ThreadWorkerPool, ProcessWorkerPool

parametrize_pool_both = functools.partial(
    pytest.mark.parametrize,
    'pool', (ThreadWorkerPool, ProcessWorkerPool)
)
parametrize_pool_thread = functools.partial(
    pytest.mark.parametrize,
    'pool', (ThreadWorkerPool,)
)
parametrize_pool_process = functools.partial(
    pytest.mark.parametrize,
    'pool', (ProcessWorkerPool,)
)

@pytest.fixture(autouse=True)
def check_resources():
    yield
    # give the pool some time to destroy threads/processes; don't poll
    # regularly as that would join() zombie threads/processes
    time.sleep(1)
    # check for zombies; while this may work on other *nixes, the semantics
    # will vary, so limit this just to Linux - non-zombies are double-checked
    # below anyway, platform-independent
    if sys.platform.startswith('alinux'):
        parent = psutil.Process()
        children = list(parent.children())
        zombies = [p for p in children if p.status() == psutil.STATUS_ZOMBIE]
        assert not zombies, \
            f"{len(zombies)} zombies found"
        assert not children, \
            f"{len(children)} extra child processes still running"
    # check for non-zombies
    assert thr.active_count() == 1, \
        f"{thr.active_count()-1} extra threads left around"
    assert not mp.active_children(), \
        f"{len(mp.active_children())} extra processes still running"

def create_dummy(name):
    def dummy():
        print(f"running dummy {name}")
    return dummy

def create_dummy_with_return(name, ret):
    def dummy_return():
        print(f"running dummy {name} :: returning: {ret}")
        return ret
    return dummy_return

def create_dummy_with_exception(name, exc):
    def dummy_exception():
        print(f"running dummy {name} :: raising: {exc}")
        raise exc()
        return not None  # shouldn't be reached
    return dummy_exception

def create_dummy_with_shared(name, sh, ret=None):
    def dummy_shared(shared):
        shared[name] = sh
        print(f"running dummy {name} :: final shared: {shared}")
        return ret
    return dummy_shared

def create_dummy_with_kwargs(name):
    def dummy_kwargs(**kwargs):
        print(f"running dummy {name} :: kwargs: {kwargs}")
        return kwargs
    return dummy_kwargs

def create_dummy_shared_kwargs(name, sh):
    def dummy_shared_kwargs(shared, **kwargs):
        print(f"running dummy {name} :: shared: {shared} :: kwargs: {kwargs}")
        shared[name] = sh
        return kwargs
    return dummy_shared_kwargs

def create_dummy_that_types(name, queue, letter=None, every=0.1, total=5):
    if not letter:
        letter = name[-1]  # last letter of name
    def dummy_typing(*, queue):
        for i in range(total):
            queue.put(str(letter))
            time.sleep(every)
    return dummy_typing

#
# Basic functionality / sanity
#

def test_sanity():
    s = sched.Sched([])

def test_default_empty():
    s = sched.Sched([])
    res = list(s.run())
    print(res)

@parametrize_pool_both()
def test_empty(pool):
    s = sched.Sched([])
    res = list(s.run(pooltype=pool))
    assert res == []

@parametrize_pool_both()
def test_task_via_init(pool):
    dummy1 = create_dummy('dummy1')
    s = sched.Sched([dummy1])
    res = list(s.run(pooltype=pool))
    assert res == [TaskRes(dummy1)]

@parametrize_pool_both()
def test_add_tasks(pool):
    dummy1 = create_dummy('dummy1')
    dummy2 = create_dummy('dummy2')
    s = sched.Sched([dummy1])
    s.add_tasks([dummy2])
    res = list(s.run(pooltype=pool))
    assert TaskRes(dummy1) in res
    assert TaskRes(dummy2) in res

@parametrize_pool_both()
def test_restart(pool):
    dummy1 = create_dummy('dummy1')
    dummy2 = create_dummy('dummy2')
    s = sched.Sched([dummy1])
    res = list(s.run(pooltype=pool))
    assert TaskRes(dummy1) in res
    s.add_tasks([dummy2])
    res = list(s.run(pooltype=pool))
    assert TaskRes(dummy2) in res

@parametrize_pool_both()
def test_return_value(pool):
    dummy1 = create_dummy_with_return('dummy1', 1234)
    s = sched.Sched([dummy1])
    res = list(s.run(pooltype=pool))
    assert res == [TaskRes(dummy1, ret=1234)]

@parametrize_pool_both()
def test_return_exception(pool):
    dummy1 = create_dummy_with_exception('dummy1', NameError)
    s = sched.Sched([dummy1])
    res = list(s.run(pooltype=pool))
    dummy1res = res[0]
    assert dummy1res.task == dummy1
    assert dummy1res.excinfo.type == NameError
    assert isinstance(dummy1res.excinfo.val, NameError)

@parametrize_pool_both()
def test_priority(pool):
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, priority=3)
    dummy2 = create_dummy('dummy2')
    sched.meta.assign_val(dummy2, priority=4)
    s = sched.Sched([dummy2, dummy1])
    res = list(s.run(pooltype=pool))
    assert res == [TaskRes(dummy1), TaskRes(dummy2)]
    sched.meta.assign_val(dummy1, priority=4)
    sched.meta.assign_val(dummy2, priority=3)
    s = sched.Sched([dummy2, dummy1])
    res = list(s.run(pooltype=pool))
    assert res == [TaskRes(dummy2), TaskRes(dummy1)]

@parametrize_pool_both()
def test_requires_provides(pool):
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, requires=['dep'], priority=1)
    dummy2 = create_dummy('dummy2')
    sched.meta.assign_val(dummy2, provides=['dep'], priority=2)
    s = sched.Sched([dummy2, dummy1])
    res = list(s.run(pooltype=pool))
    assert res == [TaskRes(dummy2), TaskRes(dummy1)]

@parametrize_pool_both()
def test_uses(pool):
    q = mp.Queue()
    dummy1 = create_dummy_that_types('dummy1', q)
    sched.meta.assign_val(dummy1, uses=['res'], priority=1, kwargs={'queue': q})
    dummy2 = create_dummy_that_types('dummy2', q)
    sched.meta.assign_val(dummy2, uses=['res'], priority=2, kwargs={'queue': q})
    s = sched.Sched([dummy1, dummy2])
    res = list(s.run(pooltype=pool, workers=1))
    assert res == [TaskRes(dummy1), TaskRes(dummy2)]
    output = ''.join(map(lambda x: q.get(), range(q.qsize())))
    assert output == '1111122222'
    #s = sched.Sched([dummy1, dummy2])
    s.add_tasks([dummy1, dummy2])
    res = list(s.run(pooltype=pool, workers=2))
    assert TaskRes(dummy1) in res
    assert TaskRes(dummy2) in res
    output = ''.join(map(lambda x: str(q.get()), range(q.qsize())))
    assert '11111' not in output
    #assert '121' in output or '212' in output

@parametrize_pool_both()
def test_shared(pool):
    dummy1 = create_dummy_with_shared('dummy1', 2345)
    sched.meta.assign_val(dummy1, requires=['dep'])
    dummy2 = create_dummy_with_shared('dummy2', 1234)
    sched.meta.assign_val(dummy2, provides=['dep'])
    s = sched.Sched([dummy2, dummy1])
    res = list(s.run(pooltype=pool))
    dummy2res, dummy1res = res  # order guaranteed by dep
    assert dummy2res == TaskRes(dummy2, shared={'dummy2': 1234})
    assert dummy1res == TaskRes(dummy1, shared={'dummy2': 1234, 'dummy1': 2345})

# TODO: user pre-set shared
#        s.add_shared(setup=123)
#        moreshared = {'cleanup': 234, 'other': 345}
#        s.add_shared(**moreshared)

@parametrize_pool_both()
def test_kwargs(pool):
    dummy1 = create_dummy_with_kwargs('dummy1')
    args = {'testargs': 1234}
    sched.meta.assign_val(dummy1, kwargs=args)
    s = sched.Sched([dummy1])
    res = list(s.run(pooltype=pool))
    assert res == [TaskRes(dummy1, ret=args)]

@parametrize_pool_both()
def test_shared_kwargs(pool):
    dummy1 = create_dummy_shared_kwargs('dummy1', 1234)
    args = {'testargs': 2345}
    sched.meta.assign_val(dummy1, kwargs=args)
    s = sched.Sched([dummy1])
    res = list(s.run(pooltype=pool))
    assert res == [TaskRes(dummy1, shared={'dummy1': 1234}, ret=args)]

@parametrize_pool_both()
def test_failed_parent(pool):
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, requires=['dep'])
    dummy2 = create_dummy_with_exception('dummy2', NameError)
    sched.meta.assign_val(dummy2, provides=['dep'])
    s = sched.Sched([dummy2, dummy1])
    with warnings.catch_warnings(record=True) as caught:
        res = list(s.run(pooltype=pool))
    assert len(res) == 1  # dummy1 didn't run
    dummy2res = res[0]
    assert dummy2res.task == dummy2
    assert dummy2res.excinfo.type == NameError
    assert isinstance(dummy2res.excinfo.val, NameError)
    assert len(caught) == 1  # only one warning
    assert caught[0].category == util.PexenWarning
    assert '1 tasks skipped due to unmet deps' in str(caught[0].message)

# TODO: broken dependencies
#       - requiring something that was not provided -> exception
#       - providing something that was not required -> warning

# TODO: disabling/enabling of dep checking before run (__debug__)

# TODO: running tasks that were not present when the pool started

# TODO: restarting the Sched instance by calling .run() again

# TODO: calling .run() multiple times (before previous one finishes)

# TODO: using spare= larger than nr. of schedulable tasks; check that the runner
#       logic doesn't end prematurely and waits for dependencies to be resolved
#       - IOW make sure the runner doesn't go into "dont add any more and wait
#         for running tasks to finish" mode purely because no more tasks can be
#         run at this time

# TODO: implicit ordering preservation; we don't make any guarantees, but after
#       set(self.tasks) gets rewritten to something ordered (ie. dict keys),
#       we can guarantee best effort first-last ordering for execution

# TODO: split above requires/provides + unmet deps into a big deps testing section

# TODO: big section on testing mutexes

# TODO: failing tasks with unreleased mutexes
# TODO: combine failing tasks with leftover deps + failing tasks with unreleased mutexes
# TODO: test MutexError (same key in claims and uses)

#
# Picklability checks
#

# Multiprocessing - unpicklable objects should generate an exception

@parametrize_pool_process()
def test_unpicklable_ret(pool):
    nonpickl = create_dummy('')
    dummy1 = create_dummy_with_return('dummy1', nonpickl)
    s = sched.Sched([dummy1])
    res = list(s.run(pooltype=pool))
    dummy1res = res[0]
    assert dummy1res.task == dummy1
    assert dummy1res.excinfo.type == AttributeError
    assert "Can't pickle" in str(dummy1res.excinfo.val)

@parametrize_pool_process()
def test_unpicklable_shared(pool):
    nonpickl = create_dummy('')
    dummy1 = create_dummy_with_shared('dummy1', nonpickl)
    s = sched.Sched([dummy1])
    res = list(s.run(pooltype=pool))
    dummy1res = res[0]
    assert dummy1res.task == dummy1
    assert dummy1res.excinfo.type == AttributeError
    assert "Can't pickle" in str(dummy1res.excinfo.val)

@parametrize_pool_process()
def test_unpicklable_ret_shared(pool):
    nonpickl = create_dummy('')
    dummy1 = create_dummy_with_shared('dummy1', nonpickl, ret=nonpickl)
    s = sched.Sched([dummy1])
    res = list(s.run(pooltype=pool))
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
    res = list(s.run(pooltype=pool))
    assert res == [TaskRes(dummy1, shared={'dummy1': nonpickl}, ret=nonpickl)]

#
# Worker Pool specific functionality
#

@pytest.mark.parametrize('reuse_task_list', [True, False],
                         ids=['reused-task-list', 'new-task-list'])
@parametrize_pool_both()
def test_pool_reuse(pool, reuse_task_list):
    dummy1 = create_dummy('dummy1')
    dummy2 = create_dummy('dummy2')
    p = pool([dummy1,dummy2]) if reuse_task_list else pool([dummy1])
    p.submit(dummy1)
    p.shutdown(wait=True)
    res = list(p.iter_results())
    assert res == [TaskRes(dummy1)]
    p.start_pool() if reuse_task_list else p.start_pool(alltasks=[dummy2])
    p.submit(dummy2)
    p.shutdown(wait=True)
    res = list(p.iter_results())
    #assert res == [TaskRes(dummy2, shared=None)]
    assert res == [TaskRes(dummy2)]

# TODO: multiple result iterators (multiple iter_results() calls)

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
    res = list(s.run(pooltype=pool))
    dummy1res = res[0]
    assert dummy1res.task == dummy1
    assert dummy1res.excinfo.type == TypeError
    assert "unexpected keyword argument" in str(dummy1res.excinfo.val)

# invalid data type being passed as attr
def test_invalid_attr_type():
    dummy1 = create_dummy('dummy1')
    sched.meta.assign_val(dummy1, requires=set('dep'))  # same type
    sched.meta.assign_val(dummy1, requires=['dep'])     # allowed conversion
    with pytest.raises(AttributeError) as exc:
        sched.meta.assign_val(dummy1, requires='dep')   # denied conversion
    assert "<class 'str'> cannot be used for requires" in str(exc.value)

#
# Performance tests
#

# TODO: schedule 10000+ of tasks, both threading and mp
