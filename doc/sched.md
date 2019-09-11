# Scheduler

The job of the scheduler is to take a list/set of tasks and execute them.

The tasks are actually python [callables](https://stackoverflow.com/questions/111234/what-is-a-callable),
such as functions, and executing them is simply calling them, like a function.
This provides a very generic interface for execution of any arbitrary tasks.

It is the job of [factories](factory.md) to help you transform other data
structures (like files on a filesystem) into tasks (callables) the scheduler
can run - see the doc for more.

```python
from pexen import sched

def greet():
    return "Hello World!"

greet_another = lambda: "Hello Another World!"

s = sched.Sched([greet, greet_another])
for result in s.run():
    print(result)
```
```
TaskRes(task=<function <lambda> at 0x7f69d79399e0>, shared={}, ret='Hello Another World!', excinfo=None)
TaskRes(task=<function greet at 0x7f69d794f170>, shared={}, ret='Hello World!', excinfo=None)
```

The power of this scheduler comes from the attributes you can specify for
callables, such as dependencies, locks/mutexes, priority, shared state, etc.
See [info on task metadata](task-meta.md) for lots of details on how to do that.

## Processing results

When you call `.run()` of a `Sched` class instance, it gives you an iterable
generator which yields task results, per the `pexen.sched.TaskRes` namedtuple.

During this time, **no new tasks are scheduled** - your code has to give control
back to `.run()` so it can schedule new tasks, process finished tasks and yield
results. This means that the execution never outruns the result processing and
that you can effectively throttle the execution by taking too long before giving
back control.

The fastest way to get all results at once is therefore
```python
results = list(s.run())
```
as `list()` iterates the generator as fast as possible.

## Pools, workers and spares

### Pool types

The scheduler needs a "pool" to run tasks. The pool abstracts the actual
callable scheduling, threading/multiprocessing logic, etc.

Currently, pexen supports these pool types:
 * `pexen.sched.pool.ThreadWorkerPool` - tasks are executed using `threading`,
   can share memory, share CWD, ..., but are bound by GIL, etc.
 * `pexen.sched.pool.ProcessWorkerPool` - tasks run using `multiprocessing`
   in separate processes and can take advantage of the extra CPU power or
   separation safety
   * any data passed (incl. shared state) however need to be picklable

```python
from pexen import sched

def greet():
    return "Hello World!"

p = sched.pool.ProcessWorkerPool()
s = sched.Sched([greet])
results = s.run(pool=p)
print(list(results))
```
(The default is `ThreadWorkerPool`.)

### Workers

You can also set how many workers (execution units) run in parallel:
```python
p = sched.pool.ProcessWorkerPool(workers=10)
```
(Default is 1, eg. serial execution.)

### Spare

As mentioned above, while your code is processing results, no new tasks are
scheduled. This may lead to worker starvation if a worker finishes while
processing results.

To combat this, the scheduler keeps some tasks "pre-scheduled", meaning the
workers are free to pick them up even while `.run()` doesn't have control.
Think of it as `make -j5` on a 4-core system.

* Too small value can result in starvation
* Too large value causes sub-optimal task ordering - all (even high priority)
  tasks are scheduled **after** any tasks in the spare queue; any locks are
  held by spare tasks even when they're not actually running, etc.
