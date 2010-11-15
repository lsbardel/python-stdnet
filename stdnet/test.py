import unittest
import types

from stdnet import orm
from stdnet.utils import import_modules

TextTestRunner = unittest.TextTestRunner


class TestCase(unittest.TestCase):
    
    def __init__(self, *args, **kwargs):
        self.orm = orm
        super(TestCase,self).__init__(*args, **kwargs)
        
    def unregister(self):
        pass
    
    def tearDown(self):
        orm.clearall()
        self.unregister()
        

        
class BenchMark:
    orm = orm
            
    def __str__(self):
        return self.__class__.__name__
        
    def setUp(self):
        self.register()
        orm.clearall()
        
    def register(self):
        pass
        

class TestLoader(unittest.TestLoader):
    cls = unittest.TestCase
    
    def __init__(self, tags = None):
        self.tags  = tags
        self.elems = []
    
    def loadTestsFromModule(self, module):
        cls = self.cls
        elems = self.elems
        for name in dir(module):
            obj = getattr(module, name)
            if (isinstance(obj, (type, types.ClassType)) and issubclass(obj, cls)):
                if self.tags:
                    load = False
                    tags = getattr(obj,'tags',None)
                    if tags:
                        for tag in tags:
                            if tag in self.tags:
                                load = True
                                break
                else:
                    load = getattr(obj,'default_run',True)
                    
                if load:
                    elems.append(self.loadTestsFromTestCase(obj))
        if self.suiteClass:
            return self.suiteClass(elems)
        else:
            return elems
        


class BenchLoader(TestLoader):
    cls = BenchMark
    suiteClass = None
    
    def loadTestsFromTestCase(self, obj):
        return obj()
    
    def loadBenchFromModules(self, modules): 
        modules = import_modules(modules)
        elems = self.elems
        for mod in modules:
            self.loadTestsFromModule(mod)
        return elems

    
def runbench(benchs):
    from timeit import Timer
    t = Timer("test()", "from __main__ import test")
    for elem in benchs:
        path = elem.__module__
        name = elem.__class__.__name__
        t = Timer("b.run()", 'from %s import %s\nb = %s()\nb.setUp()' % (path,name,name))
        t = t.timeit(elem.number)
        print('Run %15s --> %s' % (elem,t))
        