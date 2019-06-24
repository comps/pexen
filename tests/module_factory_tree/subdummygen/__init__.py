from pexen import attr, factory
  
@attr.func_attr(requires=["dummy2done"])
def dummy3():
    print("running dummy3")
    return 3

def gen_minidummy(basepath, nr):
    @attr.func_attr(callpath=basepath+[f'minidummy_{nr}'])
    def minidummy():
        print(f"running minidummy {nr}")
        return nr
    return minidummy

@factory.is_factory
def my_factory(args, **kwargs):
    """Takes care of minidummies, then lets the original module_factory
    to do the rest (incl. dummy3 above).
    """
    dummies = []
    for i in range(100,110):
        dummies.append(gen_minidummy(kwargs['callpath'], i))

    factory.is_no_longer_factory(my_factory)
    other = factory.module_factory(*args, **kwargs)
    dummies.extend(other)

    return dummies
