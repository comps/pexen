# Python EXecution ENvironment

This is a (fairly) simple framework for building anything that requires some
kind of ordered execution, such as a build system, a test suite, or similar.

## Current state

The project currently sits idle until I find a use for it. It is not under
very active development, but it's also not abandonware.

Feel free to fork it and make enhancements if you wish.

No APIs, names or anything is currently declared as stable, if you use this
project in your projects, expect having to rename/rework things some time
in the future.

## Background

The idea came from limitations of various (non-Python) test suites that used
static (ini) files for configuration, limiting options for dynamic execution,
not having parallel execution support (or a crappy make-job-like one), etc.

Thus the goal was to create a system which would support

1. Tree-based structure of tests, no single (flat) or double (main/sub) levels
1. Dependency tracking and resolution between tests
1. Parallel execution of tests with resolved dependencies
1. Dynamic runtime configuration (selecting tests based on OS, etc.)
1. Multiple results per one test (ie. 'pass', but with problems detected)
1. Persistent state keeping (for re-runs of tests, for OS reboot)
1. Some watchdog for keeping test runtime in check
1. etc.

and the idea was to utilize python as a "configuration" language.

Generalizing this, the above formed into

1. A "factory-time" logic, which would traverse a tree structure and generate
   the tests (a.k.a. execution units, python callables) based on some input
1. An "execution-time" logic, which would use a scheduler to efficiently run
   the collected tests

Further generalizing, it ultimately turned into an universal execution engine
info which you can feed any python callables and, given the right metadata,
they get scheduled and run in the right order, with the "factory-time" being
essentially just an example way of how to generate callables.

## How to use

### Scheduler

Takes a list of callables, optionally annotated by `@pexen.attr.func_attr`,
optionally with metadata (`@pexen.attr.func_attr(key1=val1, key2=val2)`) with
possible keys:

* `callpath` - not used by scheduler, but set up by the factory logic;
  uses a list to represent hierarchy level and name of the callable, for use
  by the user when processing results,
  ie. `['kernel', 'fs', 'ext4', 'test_open']`
* `requires` - a list of strings or other objects supporting comparison (`==`);
  the callable won't be run unless all of these are provided by other callables
* `provides` - a list of strings or other objects supporting comparison (`==`);
  when the callable finishes running, it "provides" these, unblocking any others
  that "require" the specified strings/objects
* `claims` - Not Implemented Yet; list of strings/objects to claim a mutex on,
  so that only one callable locking any of these can run at a time
* `priority` - a sortable (eg. int) value; given multiple callables capable of
  execution (all requires met), prefer the ones with lower priority value;
  defaults to 0
* `kwargs` - arbitrary user args; passed to the callable during execution

Each callable can either be argument-less or can define either or both of

* `shared` - a position (first) argument; a dict passed during execution - any
  changes to it will propage (`.update()`) to all "child" callables (their
  `shared` dicts); "child" as in "any other callables that require something the
  current callable provides"

  * if using `ProcessWorkerPool`, this needs to be picklable

* `**kwargs` - user-provided dict from the callable's annotated metadata,
  passed by the scheduler as keyword arguments

  * as it is passed by reference, it does not need to be picklable

The callable can also return any value, however this value needs to be picklable
if using `ProcessWorkerPool`.

The returned result to the user program is one tuple per one executed callable,
with the tuple consisting of:

* [0]: the callable object that finished executing
* [1]: its shared state (the `shared` arg) when it finished execution, or `None`
  if it aborted prematurely with an exeception
* [2]: the return value of the callable or `None` in case of exception
* [3]: `sys.exc_info()` if the callable encountered an exception, or `None` on
  success; note that some of the fields may be `None` if they failed
  picklability checks and `ProcessWorkerPool` was used

### Scheduler Example

Until I write a better set of examples, take a look at `tests/`, particularly
`tests/test_sched.py`.

```python
#!/usr/bin/env python3
  
from pprint import pprint
from time import sleep
from pexen import attr, sched

def greet():
    return "Hello World!"

@attr.func_attr(provides=[1])
def get_password(shared):
    shared['pw'] = 123456
    sleep(1)

@attr.func_attr(provides=[2], kwargs={'account': 'john'})
def get_user(shared, *, account='Unknown'):
    shared['login'] = account
    sleep(1)

@attr.func_attr(requires=[1,2])
def format_msg(shared):
    msg = f"user: {shared['login']} with pw: {shared['pw']}"
    shared.clear()  # hide evidence
    return msg

tasks = [greet, get_password, get_user, format_msg]

# takes ~2 seconds, runs with 1 worker
print("===")
s = sched.Sched(tasks)
results = list(s.run())
pprint(results)

# takes ~1 second because get_password and get_user run in parallel
print("===")
s = sched.Sched(tasks)
results = list(s.run(workers=10))
pprint(results)

# uses multiprocessing instead of threading
# also shows how to iterate over results
# also makes the greeting go first (as it doesn't require anything)
print("===")
attr.assign_val(greet, priority=-1)
s = sched.Sched(tasks)
for res in s.run(pooltype=sched.ProcessWorkerPool):
    print(res)
```

## Limitations

Needs python 3.6+, mostly due to quality-of-life features. Functionally,
it could be ported to python 2.7 as it currently uses only basic `threading`
and `multiprocessing` features (eg. no `futures`), at the expense of code
readability.

* Parallel result pipelining
  * Wish
    * A thread-safe process-safe interface to get results from running tasks
    * The user can then spawn a separate custom pool of threads/processes that
      query this interface and post-process results returned from tasks, in
      parallel
      * While other tasks are still running, not after everything finishes
  * Why Not
    * There are process-unsafe metadata returned to the user, namely the
      callable objects that were executed, exception objects, etc.
      * These are safe to pass only to the thread/process that added the task
    * There is a simple workaround: simply take care of the metadata in the
      thread that added the tasks and pass on picklable data (ie. retvals
      from the executed callables) to the custom process pool for further
      post-processing

## Wishlist

* Scheduling groups of exclusivity
  * Pass groups of callables instead of just callables to the scheduler
  * Have only one group running at a time
    * As it finishes, destroy the worker pool, create a new one for the
      new group
  * Allows to specify nr. of workers/spare and pool type (thread/mp) used
    on a per group basis!
  * Useful when a set of tasks is very different from the rest, ie. testing
    virtual machines (5 workers) versus testing syscalls (100 workers), and one
    global setting cannot cover both.
  * Also useful to logically split mutually exclusive things that need
    uninterrupted / unconditional access to the OS without defining "claims"
    to lots of tests.
  * Also useful for tasks that should logically run "together", possibly
    needing sequential execution due to formal requirements.

* Conditional provides
  * Don't fulfill requires of dependent tasks if the current task (callable)
    fails
    * Fails = tracebacks? Returns a specific value? Returns anything other than
      a specific value?
    * Have an attr with a comparison callable to determine failure?
