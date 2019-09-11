#
# Planner / Sched() API
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
# Basic functionality / sanity
#

def test_sanity():
    s = sched.Sched([])

def test_empty_default():
    s = sched.Sched([])
    res = list(s.run())
    print(res)

@parametrize_pool_both()
def test_empty(pool):
    s = sched.Sched([])
    res = list(s.run(pool()))
    assert res == []

def test_task_via_init_default():
    dummy1 = create_dummy('dummy1')
    s = sched.Sched([dummy1])
    res = list(s.run())
    assert res == [TaskRes(dummy1)]

@parametrize_pool_both()
def test_task_via_init(pool):
    dummy1 = create_dummy('dummy1')
    s = sched.Sched([dummy1])
    res = list(s.run(pool()))
    assert res == [TaskRes(dummy1)]

@parametrize_pool_both()
def test_add_tasks(pool):
    dummy1 = create_dummy('dummy1')
    dummy2 = create_dummy('dummy2')
    s = sched.Sched([dummy1])
    s.add_tasks([dummy2])
    res = list(s.run(pool()))
    assert TaskRes(dummy1) in res
    assert TaskRes(dummy2) in res

@parametrize_pool_both()
def test_restart(pool):
    dummy1 = create_dummy('dummy1')
    dummy2 = create_dummy('dummy2')
    s = sched.Sched([dummy1])
    res = list(s.run(pool()))
    assert TaskRes(dummy1) in res
    s.add_tasks([dummy2])
    res = list(s.run(pool()))
    assert TaskRes(dummy2) in res

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

# TODO: try .run() with empty task set



#
# Performance tests
#

# TODO: schedule 10000+ of tasks, both threading and mp
#        - in test_perf.py
