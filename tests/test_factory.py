import pytest
import sys
import functools
import threading as thr
import multiprocessing as mp
import time
import psutil

from pexen import attr, factory
from tests import module_factory_tree

def test_module_factory():
    funcs = factory.module_factory(startfrom=module_factory_tree)
    dummy2 = next(x for x in funcs if x.__name__ == 'dummy2')
    dummy3 = next(x for x in funcs if x.__name__ == 'dummy3')
    assert attr.get_provides(dummy2) == {'dummy2done',}
    assert attr.get_requires(dummy3) == {'dummy2done',}
    res = [x() for x in funcs]
    assert all([i in res for i in range(1,5)])  # dummies
    assert all([i in res for i in range(100,110)])  # minidummies
