from .test import TestCase, TestSuiteRunner
from .bench import BenchMark, BenchSuiteRunner



        
TEST_TYPES = {'regression': TestSuiteRunner,
              'bench': BenchSuiteRunner,
              'profile': None}
