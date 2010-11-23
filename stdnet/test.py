import os
import sys
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
        
        
class StdNetTestSuiteRunner(unittest.TestSuite):
    verbosity = 0
    
    def run(self, result):
        old_config = self.setup_django_databases()
        result = unittest.TestSuite.run(self,result)
        self.teardown_django_databases(old_config)
        return result
    
    def setup_django_databases(self, **kwargs):
        from django.db import connections
        old_names = []
        mirrors = []
        for alias in connections:
            connection = connections[alias]
            # If the database is a test mirror, redirect it's connection
            # instead of creating a test database.
            if connection.settings_dict['TEST_MIRROR']:
                mirrors.append((alias, connection))
                mirror_alias = connection.settings_dict['TEST_MIRROR']
                connections._connections[alias] = connections[mirror_alias]
            else:
                old_names.append((connection, connection.settings_dict['NAME']))
                connection.creation.create_test_db(self.verbosity, autoclobber=True)
        return old_names, mirrors
    
    def teardown_django_databases(self, old_config, **kwargs):
        from django.db import connections
        old_names, mirrors = old_config
        # Point all the mirrors back to the originals
        for alias, connection in mirrors:
            connections._connections[alias] = connection
        # Destroy all the non-mirror databases
        for connection, old_name in old_names:
            connection.creation.destroy_test_db(old_name, self.verbosity)


class TestLoader(unittest.TestLoader):
    cls = unittest.TestCase
    
    def __init__(self, tags = None):
        self.tags  = tags
        self.elems = []
        try:
            import django
            self.setup_django()
        except ImportError:
            pass
    
    def setup_django(self):
        path = os.path.split(os.path.abspath(__file__))[0]
        tpath = os.path.join(path,'tests','testapplications')
        sys.path.insert(0,tpath)
        os.environ['DJANGO_SETTINGS_MODULE'] = 'djangotestapp.settings'
        self.suiteClass = StdNetTestSuiteRunner
        
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
        