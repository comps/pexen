import pytest
import sys
import functools
import threading as thr
import multiprocessing as mp
import time
import psutil
import warnings

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
    if sys.platform.startswith('linux'):
        parent = psutil.Process()
        children = list(parent.children())
        zombies = [p for p in children if p.status() == psutil.STATUS_ZOMBIE]
        assert not zombies, \
            f"{len(zombies)} zombies found"
        assert not children, \
            f"{len(children)} extra child processes still running"
    # check for non-zombies
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

def create_dummy_that_types(name, letter=None, every=0.1, total=5):
    if not letter:
        letter = name[-1]  # last letter of name
    def dummy_typing(*, q):
        for i in range(total):
            q.put(str(letter))
            time.sleep(every)
    return dummy_typing


# internal
def _scope_filter(name):
    return any((
        name.startswith('parametrize_pool'),
        name.startswith('create_dummy'),
    ))

__all__ = list(filter(_scope_filter, dir()))
