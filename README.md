# Python EXecution ENvironment

This is a (fairly) simple framework for building anything that requires some
kind of ordered execution, such as a build system, a test suite, or similar.

## How to use it

See further topics (in this order):

1. [Scheduler](doc/sched.md)
1. [Task metadata](doc/task-meta.md)
1. [Factory](doc/factory.md)

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
essentially just an example way of how to generate execution units.

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

(In no specific order.)

* Debug support
  * While the tasks are running, ie. during results iteration
  * Currently running tasks
    * Impossible to get, but the scheduler could keep track of what tasks
      were put on frontline and remove them from tracking when they finish
  * Currently held locks
  * ...
  * Very useful to find badly behaving tasks

* Shutdown / interrupt support
  * Abort execution based on ie. fatal result from some task
  * As `.shutdown()` or via context manager?

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

* HierarchyLock, a special object for `uses`/`claims`
  * If one task locks `/proc/sys/net`, another task blocks while trying to lock
    `/proc/sys/net/ipv4/ip_forward`, without having to try to lock its parents.
    * Filesystem paths are just an example, `list` would be used instead of `/`
  * Should be easy to implement
    * Hack everything in __eq__ of the object instance
    * No changes to sched needed

* Adding new tasks on-the-fly
  * While processing results from the `Sched.run()` iterator, add new tasks
  * Useful when the overall task set depends on result(s) from some tasks
  * `pexen.sched.pool` support already in place
    * But proper `Sched` support would need heavy rewrites, removal of sanity
      checks, etc.
      * I did the rewrite; took 6-8 hours and the code was incomprehensible
      * Refactoring of the whole `pexen.sched` would be needed

* A post-processor for factories
  * A task that requires some generic resource, ie. `httpd`
  * The post-processor would find (somehow) another, best suitable, task that
    provides it, adding it to the tasks for execution
  * Similar to how Debian packages can depend on a virtual package like
    `mail-transport-agent` and have multiple packages (`exim4`, `postfix`, etc.)
    provide it

* "UseCases" that would glue Factory and Scheduler
  * Ie. "test suite UseCase" for running a test suite, collecting and formatting
    results, storing xUnit results in a file, supporting resume from
    an interrupted execution, etc.

* NetworkWorkerPool, based on ProcessWorkerPool
  * Can listen/connect across network
  * Addresses, ports, etc. defined in pool constructor, by the user
  * Transmits picklable objects, like ProcessWorkerPool, but over network
  * Could open further cans of worms
    * "Run this one setup task on all nodes"
    * "Define node properties and run certain tasks only on certain nodes"
    * etc.
