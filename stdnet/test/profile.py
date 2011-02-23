from .test import TestLoader, TestSuiteRunner
from .bench import BenchMark


class ProfileTest(BenchMark):
    pass


class ProfileSuite(object):
    
    def __init__(self, tests):
        self.tests = tests
          
    
class ProfileLoader(TestLoader):
    testClass = ProfileTest
    suiteClass = ProfileSuite
    
    def loadTestsFromTestCase(self, obj):
        return obj()
    
    
class ProfileSuiteRunner(TestSuiteRunner):
    Loader = ProfileLoader
    
    def run_suite(self, suite):
        try:
            import cProfile
            import pstats
        except ImportError:
            print('You need to install the python profiler installed to run profile tests')
            exit(0)
        profile = cProfile.runctx
        n = 0
        stats = []
        for elem in suite.tests:
            elem.setUp()
            fname = "Profile{0}.prof".format(n)
            stats.append(fname)
            n += 1
            profile("elem.run()",globals(),locals(),fname)
            elem.tearDown()
        
        if stats:
            s = pstats.Stats(stats[0])
            for st in stats[1:]:
                s.add(st)
            s.strip_dirs().sort_stats("time").print_stats()
        else:
            print('Nothing done')
        
