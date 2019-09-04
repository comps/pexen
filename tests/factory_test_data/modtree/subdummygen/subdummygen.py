from pexen import sched
  
@sched.meta.func_meta
def dummy4():
    print("running dummy4")
    return 4
