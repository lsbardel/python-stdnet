from timeit import Timer

from stdnet.utils import import_modules


def loadBenchFromModules(modules):
    modules = import_modules(modules)
    benchs  = []
    for mod in modules:
        path = mod.__name__
        for name in mod.__dict__:
            elem = getattr(mod,name)
            if isinstance(elem,BenchMarkType) and not elem is BenchMark:
                benchs.append((path,elem()))
    return benchs




def run(benchs):
    t = Timer("test()", "from __main__ import test")
    for path,elem in benchs:
        name = elem.__class__.__name__
        t = Timer("b.run()", 'from %s import %s\nb = %s()\nb.setUp()' % (path,name,name))
        t = t.timeit(elem.number)
        print('Run %15s --> %s' % (elem,t))


class BenchMarkType(type):
    pass
    
    
class BenchMark(object):
    __metaclass__ = BenchMarkType
    number = 100
    
    def __str__(self):
        return self.__class__.__name__
    
    def setUp(self):
        pass
    
    def run(self):
        pass
    
    
def makebench(f):
    
    class b(BenchMark):
        def run(self):
            f()
    b.__name__ = f.__name__
    b.abstract = False
    return b