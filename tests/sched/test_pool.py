import pytest

from pexen.sched import TaskRes

from tests.sched.common import *
from tests.sched.common import check_resources

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
    p.start_pool() if reuse_task_list else p.start_pool(tasks=[dummy2])
    p.submit(dummy2)
    p.shutdown(wait=True)
    res = list(p.iter_results())
    assert res == [TaskRes(dummy2)]

# TODO: pool.shutdown() while active tasks are still running
#        - iter_results() should work just fine

# TODO: iter_results() after all tasks have finished (pool.empty() == True)

# TODO: tests for adding tasks during runtime; both thread and pool
