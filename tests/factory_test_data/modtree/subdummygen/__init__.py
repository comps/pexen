from pexen import sched

@sched.meta.func_attr(requires=["dummy2done"])
def dummy3():
    print("running dummy3")
    return 3

# not included itself, does not have sched metadata
def gen_minidummy(nr):
    @sched.meta.func_attr
    def minidummy():
        print(f"running minidummy {nr}")
        return nr
    globals()[f'minidummy{nr}'] = minidummy

for i in range(100,110):
    gen_minidummy(i)
