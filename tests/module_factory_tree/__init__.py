from pexen import attr

@attr.func_attr
def dummy1():
    print("running dummy1")
    return 1

def nondummy():
    """This will not get included as valid callable."""
    return 999
