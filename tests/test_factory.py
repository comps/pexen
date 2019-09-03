from pexen import factory, sched
from pexen.factory import get_callpath

from tests.factory_test_data import modtree as modtree_root

def test_modtree():
    f = factory.ModTreeFactory(modtree_root)
    funcs = f.walk()
    dummy2 = next(x for x in funcs if x.__name__ == 'dummy2')
    dummy3 = next(x for x in funcs if x.__name__ == 'dummy3')
    assert sched.attr.get_provides(dummy2) == {'dummy2done',}
    assert sched.attr.get_requires(dummy3) == {'dummy2done',}
    res = [x() for x in funcs]
    assert all([i in res for i in range(1,5)])  # dummies
    assert all([i in res for i in range(100,110)])  # minidummies
    # callpaths
    dummy1 = next(x for x in funcs if x.__name__ == 'dummy1')
    minidummy100 = next(x for x in funcs if x.__name__ == 'minidummy') # first
    dummy4 = next(x for x in funcs if x.__name__ == 'dummy4')
    assert get_callpath(dummy1) == ['dummy1']
    assert get_callpath(dummy2) == ['dummygen', 'dummy2']
    assert get_callpath(dummy3) == ['subdummygen', 'dummy3']
    assert get_callpath(minidummy100) == ['subdummygen', 'minidummy100']
    assert get_callpath(dummy4) == ['subdummygen', 'subdummygen', 'dummy4']
