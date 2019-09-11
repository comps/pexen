# Factory

Factories are task (callable) generators that take an arbitrary input and
produce scheduler-compatible tasks from it.

The input can be a filesystem directory tree of executable files (being wrapped
in `subprocess.call()`), a tree of python modules (with tasks as python
functions), a JSON/YAML text file defining tasks, anything that can be machine
processed, ... provided there is a factory capable of processing it.

This means you can build source code, execute test suites, clean up remote
databases, etc. as long as there is a factory abstracting it as standalone tasks
(callables) for the scheduler.

```python
from pexen import factory
from pprint import pprint

from tests.factory_test_data import modtree as modtree_root

f = factory.ModTreeFactory()
funcs = list(f(modtree_root))
pprint(funcs)
```
```
[<function dummy1 at 0x7f9e5f908f80>,
 <function dummy2 at 0x7f9e5f90f950>,
 <function dummy3 at 0x7f9e5f91a050>,
 <function gen_minidummy.<locals>.minidummy at 0x7f9e5f91a0e0>,
 ...
 <function gen_minidummy.<locals>.minidummy at 0x7f9e5f91a5f0>,
 <function dummy4 at 0x7f9e5f91a710>]
```
Here, `ModTreeFactory` is used to traverse a python package, its modules and
any subpackages, collecting callables tagged with `@pexen.sched.meta.func_meta`
decorator.

## The Factory Specification

There is a semi-formal specification of the API and the behavior a factory class
should have, for anybody implementing custom factories and/or executing other
factories from within factories (such as to process a `.py` file as a module
while traversing a filesystem directory tree).

See the specification [in a separate file](factory-spec.txt).
