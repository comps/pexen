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

def test_modtree_callpath():
    f = factory.ModTreeFactory()
    funcs = list(f(modtree_root))
    res = [x() for x in funcs]
    dummy1 = next(x for x in funcs if x.__name__ == 'dummy1')
    dummy2 = next(x for x in funcs if x.__name__ == 'dummy2')
    dummy3 = next(x for x in funcs if x.__name__ == 'dummy3')
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
    fs_prefix = 'tests/factory_test_data/filesystem'
    def test_filesystem():
        f = factory.FilesystemFactory()
        funcs = list(f(fs_prefix))
        res = [x() for x in funcs]
        for r in res:
            assert isinstance(r, subprocess.CompletedProcess)
            assert r.returncode == 0
        assert len(res) == 3  # hidden didn't run
        stdouts = [x.stdout.decode().strip() for x in res]
        assert 'dummy1' in stdouts
        assert 'dummy2' in stdouts
        assert 'linkdummy' in stdouts
        assert 'nodummy' not in stdouts
        assert stdouts.index('dummy1') < stdouts.index('dummy2')

    def test_filesystem_callpath():
        f = factory.FilesystemFactory()
        funcs = list(f(fs_prefix))
        res = dict((x,x()) for x in funcs)
        prefix_list = fs_prefix.strip('/').split('/')
        dummy1 = next(x for x in res if 'dummy1' in res[x].stdout.decode())
        assert factory.get_callpath(dummy1) == prefix_list+['dummy1.sh']
        dummy2 = next(x for x in res if 'dummy2' in res[x].stdout.decode())
        assert factory.get_callpath(dummy2) == prefix_list+['subdummy', 'dummy2']

