from pexen import sched

@sched.meta.func_meta
def dummy1():
    print("running dummy1")
    return 1

def nondummy():
    """This will not get included as valid callable."""
    return 999
