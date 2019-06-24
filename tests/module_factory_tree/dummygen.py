from pexen import attr
  
@attr.func_attr(provides=["dummy2done"])
def dummy2():
    print("running dummy2")
    return 2

