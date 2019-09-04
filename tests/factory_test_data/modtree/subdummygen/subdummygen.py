from pexen import sched
  
@sched.meta.func_attr
def dummy4():
    print("running dummy4")
    return 4
