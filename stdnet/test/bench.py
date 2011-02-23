from timeit import Timer

from stdnet import orm

from .test import TestLoader, TestSuiteRunner


class BenchMark(object):
    orm = orm
            
    def __str__(self):
        return self.__class__.__name__
        
    def setUp(self):
        self.register()
        orm.clearall()
        self.initialise()
        
    def initialise(self):
        pass
        
    def register(self):
        pass
    
    def run(self):
        raise NotImplementedError
    
    def tearDown(self):
        pass
    

class BenchSuite(object):
    
    def __init__(self, tests):
        self.tests = tests
          
    
class BenchLoader(TestLoader):
    testClass = BenchMark
    suiteClass = BenchSuite
    
    def loadTestsFromTestCase(self, obj):
        return obj()
    
    
class BenchSuiteRunner(TestSuiteRunner):
    Loader = BenchLoader
    
    def run_suite(self, suite):
        t = Timer("test()", "from __main__ import test")
        for elem in suite.tests:
            path = elem.__module__
            name = elem.__class__.__name__
            t = Timer("b.run()", 'from %s import %s\nb = %s()\nb.setUp()' % (path,name,name))
            t = t.timeit(elem.number)
            print('Run %15s --> %s' % (elem,t))
            