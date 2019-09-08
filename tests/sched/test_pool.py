import pytest

from pexen.sched import TaskRes
from pexen.sched.pool import PoolError

from tests.sched.common import *
from tests.sched.common import check_resources

@parametrize_pool_both()
def test_pool_reuse(pool):
    dummy1 = create_dummy('dummy1')
    dummy2 = create_dummy('dummy2')
    p = pool()
    p.register([dummy1])
    p.start()
    p.submit(dummy1)
    p.shutdown(wait=True)
    assert not p.empty()
    assert not p.alive()
    res = list(p.iter_results())  # also tests results-after-shutdown
    assert p.empty()
    assert res == [TaskRes(dummy1)]
    p.register([dummy2])
    p.start()
    p.submit(dummy1)
    p.submit(dummy2)
    p.shutdown(wait=True)
    assert not p.empty()
    assert not p.alive()
    res = list(p.iter_results())  # also tests results-after-shutdown
    assert p.empty()
    assert TaskRes(dummy1) in res
    assert TaskRes(dummy2) in res

@parametrize_pool_both()
def test_shutdown_without_gather(pool):
    dummy1 = create_dummy('dummy1')
    p = pool()
    p.register([dummy1])
    p.start()
    p.submit(dummy1)
    p.shutdown(wait=True)

@parametrize_pool_thread()
def test_onthefly_thread(pool):
    dummy1 = create_dummy('dummy1')
    dummy2 = create_dummy('dummy2')
    p = pool()
    p.register([dummy1])
    p.start()
    p.submit(dummy1)
    p.submit(dummy2)
    assert p.alive()
    p.shutdown()
    res = list(p.iter_results())
    assert not p.alive()
    assert TaskRes(dummy1) in res
    assert TaskRes(dummy2) in res

@parametrize_pool_process()
def test_onthefly_process(pool):
    dummy1 = create_dummy('dummy1')
    dummy2 = create_dummy('dummy2')
    p = pool()
    p.register([dummy1])
    p.start()
    with pytest.raises(PoolError) as exc:
        p.register([dummy2])
    assert "Cannot register tasks while the pool is running" in str(exc.value)
    p.submit(dummy1)
    with pytest.raises(PoolError) as exc:
        p.submit(dummy2)
    assert "Cannot submit unregistered task" in str(exc.value)
    assert p.alive()
    p.shutdown()
    res = list(p.iter_results())
    assert not p.alive()
    assert TaskRes(dummy1) in res

@parametrize_pool_thread()
def test_submit_tail(pool):
    dummy1 = create_dummy('dummy1')
    dummy2 = create_dummy('dummy2')
    p = pool()
    p.register([dummy1,dummy2])
    p.start()
    p.submit(dummy1)
    res = p.iter_results()
    assert TaskRes(dummy1) == next(res)
    p.submit(dummy2)
    assert TaskRes(dummy2) == next(res)
    p.shutdown()
    with pytest.raises(StopIteration):
        next(res)
