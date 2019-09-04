from pexen import sched

@sched.meta.func_meta(provides=["dummy2done"])
def dummy2():
    print("running dummy2")
    return 2
