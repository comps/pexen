# Scheduler - task metadata

## Callable attribute

Each task (callable) can have additional metadata associated with it, tweaking
its execution.

These are represented by the `pexen.sched.meta.SchedMeta` structure, which
defines

* `requires` - a list of strings or other objects supporting comparison (`==`);
  the callable won't be run unless all of these are provided by other callables
* `provides` - a list of strings or other objects supporting comparison (`==`);
  when the callable finishes running, it "provides" these, unblocking any others
  that "require" the specified strings/objects
* `claims` - list of strings/objects to claim a read-write lock on, so that only
  one callable locking any of these can run at a time
* `uses` - like `claims`, but multiple callables using these strings/objects can
  run simultaneously, see: read-only lock
* `priority` - a sortable (eg. int) value; given multiple callables capable of
  execution, prefer the ones with lower priority value; defaults to 0
* `kwargs` - arbitrary user-defined keyword arguments; passed to the callable
  during execution

```python
from pexen import sched
from random import choice

def greet1():
    return "Hello First World!"

def greet2():
    return "Hello Second World!"

def greet3():
    return "Hello Third World!"

m1 = sched.meta.SchedMeta(priority=choice([0,10]))
sched.meta.assign_meta(greet1, m1)

m2 = sched.meta.SchedMeta()
m2.priority = choice([0,10])
sched.meta.assign_meta(greet2, m2)

sched.meta.assign_val(greet3, priority=choice([0,10]))

s = sched.Sched([greet1, greet2, greet3])
for result in s.run():
    print(result)
```
```
TaskRes(task=<function greet2 at 0x7f9edafe5170>, shared={}, ret='Hello Second World!', excinfo=None)
TaskRes(task=<function greet3 at 0x7f9edaf3ecb0>, shared={}, ret='Hello Third World!', excinfo=None)
TaskRes(task=<function greet1 at 0x7f9edafcfe60>, shared={}, ret='Hello First World!', excinfo=None)

TaskRes(task=<function greet3 at 0x7f0479785cb0>, shared={}, ret='Hello Third World!', excinfo=None)
TaskRes(task=<function greet1 at 0x7f0479816e60>, shared={}, ret='Hello First World!', excinfo=None)
TaskRes(task=<function greet2 at 0x7f047982c170>, shared={}, ret='Hello Second World!', excinfo=None)
```

For simplicity, you can also use a function decorator:
```python
@sched.meta.func_meta(priority=10, kwargs={'version': 'Last'})
def greet_ver(**kwargs):
    return f"Hello {kwargs['version']} World!"
```
```
TaskRes(task=<function greet_ver at 0x7f60ba5dde60>, shared={}, ret='Hello Last World!', excinfo=None)
```

## Return value

The task can return anything it deems suitable for its function. The scheduler
doesn't make use of it in any way and passes it directly, via `TaskRes`, to the
user for further processing - see above:
```python
from pexen import sched

def greet():
    return {'set': {1,2,3}, 'list': [1,2,3]}

s = sched.Sched([greet])
print(list(s.run()))
```
```
[TaskRes(task=<function greet at 0x7f8ea0d09ef0>, shared={}, ret={'set': {1, 2, 3}, 'list': [1, 2, 3]}, excinfo=None)]
```

When using `ProcessWorkerPool`, the returned object needs to be picklable.

## Shared state

If a task (callable) defines a positional argument, a `dict` of shared state
is passed to it during execution. This is distinct from the metadata-held
kwargs.
```python
def greet(shared):
    pass

def greet_ver(shared, **kwargs):
    pass
```
The task is free to modify the shared `dict`, add new keys, append to values,
etc. - it is private to the running task while it's running.

When the task finishes, the scheduler looks at any tasks which depended on
the current one (by requiring something the current one provides) and performs
`dict.update()` of the shared state of each new task with the shared state of
the task that finished.

This effectively transfers any data that the "parent" task wrote to the shared
`dict` to its "children". Note that tasks which don't directly depend on the
finished tasks are never touched.

...  
You can pre-set the shared state for all tasks with `add_shared`:
```python
s = sched.Sched([greet, greet_ver])
s.add_shared(wtype='Terrestrial', build=666)
for result in s.run():
    ...
```

The task's shared state is also present in `TaskRes` of the task that just
finished:
```python
TaskRes(task=<function greet at 0x7f524a43acb0>, shared={'msg': 'Hello World!'}, ret=None, excinfo=None)
```

To get the full picture,
```python
from pexen import sched

@sched.meta.func_meta(provides=['worldtype'])
def greet(shared):
    shared['msg'] = "Hello World!"
    shared['wtype'] = 'Silicate'

@sched.meta.func_meta(requires=['worldtype'], kwargs={'version': 'Last'})
def greet_ver(shared, **kwargs):
    shared['msg'] = f"Hello {kwargs['version']} {shared['wtype']} World!"

@sched.meta.func_meta(kwargs={'version': 'Last'})
def greet_ver_nodep(shared, **kwargs):
    shared['msg'] = f"Hello {kwargs['version']} {shared['wtype']} World!"

s = sched.Sched([greet, greet_ver, greet_ver_nodep])
s.add_shared(wtype='Terrestrial')
for result in s.run():
    print(result)
```
```
TaskRes(task=<function greet_ver_nodep at 0x7f08d7f03050>, shared={'wtype': 'Terrestrial', 'msg': 'Hello Last Terrestrial World!'}, ret=None, excinfo=None)
TaskRes(task=<function greet at 0x7f08d7f7c170>, shared={'wtype': 'Silicate', 'msg': 'Hello World!'}, ret=None, excinfo=None)
TaskRes(task=<function greet_ver at 0x7f08d7ed5cb0>, shared={'wtype': 'Silicate', 'msg': 'Hello Last Silicate World!'}, ret=None, excinfo=None)
```
As you can see, `shared['wtype']` propagated from `greet` to `greet_ver` and,
moreover, `shared['msg']` from `greet` got overwritten by `greet_ver` within
its own shared state.

As a side note, `kwargs` is not part of `TaskRes` as it was explicitly pre-set
by the user, whereas the shared state might be dynamically populated during
execution, based on where the task sits within dependency chains.

Finally, note that when using `ProcessWorkerPool`, any additions or changes
(of keys or values) to the shared `dict` need to be picklable.

## Callable arguments

The scheduler makes an effort to identify whether a task (callable) defines
a shared state positional argument, kwargs keyword argument(s), neither or both.
The task can thus have no arguments, just the shared one, just kwargs, or both.

```python
def no_args():
    pass

def shared_only(shared):
    pass

def args_only(**kwargs):
    pass

def both(shared, **kwargs):
    pass
```

Note that you can of course expand kwargs and use default values:
```python
def args_only(*, wtype, build=123):
    pass

def both(shared, wtype, build=123):
    pass
```

## Task failure, controlled failure

When a task fails (raises an exception), `TaskRes().excinfo` is populated with
an `pexen.sched.ExceptionInfo` namedtuple, which contains verbatim
`sys.exc_info` of the exact error, just within named fields.

```python
from pexen import sched

def greet_exc():
    test = nonexistent
    return "Hello World!"

s = sched.Sched([greet_exc])
print(list(s.run()))
```
```
[TaskRes(task=<function greet_exc at 0x7fccaeb8fef0>, shared={}, ret=None, excinfo=ExceptionInfo(type=<class 'NameError'>, val=NameError("name 'nonexistent' is not defined"), tb=<traceback object at 0x7fccae46ce60>))]
```

When a task fails with exception, any locks held are **not** released and any
`provides` it would normally provide are **not** satisfied. This means any tasks
which need to lock a specific resource and tasks which depend on the failed one
are **not** executed, with a `pexen.PexenWarning` warning being emitted.
```python
from pexen import sched

@sched.meta.func_meta(priority=-1, provides=['greeting'], claims=['world'])
def greet_exc():
    test = nonexistent
    return "Hello World!"

@sched.meta.func_meta(requires=['greeting'])
def greet_signature():
    return "Yours, Mr. Builder"

@sched.meta.func_meta(claims=['world'])
def greet_underlings():
    return "Hello Minions!"

s = sched.Sched([greet_exc, greet_signature, greet_underlings])
for result in s.run():
    print(result)
```
```
TaskRes(task=<function greet_exc at 0x7feb19d66170>, shared={}, ret=None, excinfo=ExceptionInfo(type=<class 'NameError'>, val=NameError("name 'nonexistent' is not defined"), tb=<traceback object at 0x7feb19633410>))
pexen/sched/planner.py:374: PexenWarning: 1 tasks skipped due to unmet deps caused by parent exception
  category=util.PexenWarning)
pexen/pexen/sched/planner.py:379: PexenWarning: 1 locks still held at exit due to parent exception
  category=util.PexenWarning)
```

### TaskFailError

You can override this by catching the exception within the task code and raising
`pexen.sched.TaskFailError` or your own subclassed from it. This signals the
scheduler that the task failed in a controller manner and that the user took
care of properly cleaning up any resources, etc.
```python
...
    try:
        test = nonexistent
    except NameError:
        raise sched.TaskFailError("Hello Harsh World!")
    return "Hello World!"
...
```
```
TaskRes(task=<function greet_exc at 0x7fd9ac22d170>, shared={}, ret=None, excinfo=ExceptionInfo(type=<class 'pexen.sched.common.TaskFailError'>, val=TaskFailError('Hello Harsh World!'), tb=<traceback object at 0x7fd9abafa410>))
TaskRes(task=<function greet_underlings at 0x7fd9ac1b4050>, shared={}, ret='Hello Minions!', excinfo=None)
TaskRes(task=<function greet_signature at 0x7fd9ac186cb0>, shared={}, ret='Yours, Mr. Builder', excinfo=None)
```

You can also choose which properties (deps, locks) are in a controlled/expected
state (all default to `True`):
```python
...
    raise sched.TaskFailError("Hello Harsh World!", deps_ok=False, locks_ok=True)
...
```
This way, you can ie. unlock all held resources while blocking child tasks
from running.

### A decorator for TaskFailError

For simplicity, if you just want any function/callable to always fail
controllably regardless of the raised exception:
```python
@sched.task_fail_wrap
def greet_exc():
    ...

@sched.task_fail_wrap(deps_ok=False)
def greet_exc2():
    ...

class MyException(sched.TaskFailError):
    pass

@sched.task_fail_wrap(reraise=MyException)
def greet_exc3():
    ...
```
Note that by using the decorator, you lose any ability to pass data
(ie. a failed test result) from the failed task, whereas by subclassing
`TaskFailError` and raising it, you can pass data as its args / attributes.

...

Alternatively to `TaskFailError`, you can silence the warnings using standard
pythonic means, see [documentation on warning control](https://docs.python.org/3/library/warnings.html).

Note that if any of the exception type/value/traceback fields are not picklable,
they will be replaced by `None` in the returned `ExceptionInfo` structure.
In the worst case, you may get an empty `ExceptionInfo` tuple. However, this
still tells you an exception occured, as opposed to `excinfo` itself being
`None`.
