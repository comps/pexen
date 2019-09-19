import os
import subprocess
from pexen import factory, sched

from tests.factory_test_data import modtree as modtree_root

def test_modtree():
    f = factory.ModTreeFactory()
    funcs = list(f(modtree_root))
    dummy2 = next(x for x in funcs if x.__name__ == 'dummy2')
    dummy3 = next(x for x in funcs if x.__name__ == 'dummy3')
    assert sched.meta.get_provides(dummy2) == {'dummy2done',}
    assert sched.meta.get_requires(dummy3) == {'dummy2done',}
    res = [x() for x in funcs]
    assert all([i in res for i in range(1,5)])  # dummies
    assert all([i in res for i in range(100,110)])  # minidummies
    # callpaths
    dummy1 = next(x for x in funcs if x.__name__ == 'dummy1')
    minidummy100 = next(x for x in funcs if x.__name__ == 'minidummy') # first
    dummy4 = next(x for x in funcs if x.__name__ == 'dummy4')
    assert factory.get_callpath(dummy1) == ['dummy1']
    assert factory.get_callpath(dummy2) == ['dummygen', 'dummy2']
    assert factory.get_callpath(dummy3) == ['subdummygen', 'dummy3']
    assert factory.get_callpath(minidummy100) == ['subdummygen', 'minidummy100']
    assert factory.get_callpath(dummy4) == ['subdummygen', 'subdummygen', 'dummy4']

def test_modtree_fnmatch():
    f = factory.ModTreeFactory()
    nomatch = set(f(modtree_root))
    assert 'test_metaless_dummy' not in (x.__name__ for x in nomatch)
    f = factory.ModTreeFactory(match='test_*')
    match = set(f(modtree_root))
    assert 'test_metaless_dummy' in (x.__name__ for x in match)

if os.path.exists('/bin/sh'):
    def test_filesystem():
        f = factory.FilesystemFactory()
        funcs = list(f('tests/factory_test_data/filesystem'))
        res = [x() for x in funcs]
        for r in res:
            assert isinstance(r, subprocess.CompletedProcess)
            assert r.returncode == 0
        assert len(res) == 2  # hidden didn't run
        assert 'dummy1' in res[0].stdout.decode()  # dummy1 is always first
        assert 'dummy2' in res[1].stdout.decode()
