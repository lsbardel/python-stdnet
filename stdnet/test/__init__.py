from .test import TestCase, TestMultiFieldMixin, TestSuiteRunner
from .bench import BenchMark, BenchSuiteRunner
from .profile import ProfileTest, ProfileSuiteRunner



        
TEST_TYPES = {'regression': TestSuiteRunner,
              'bench': BenchSuiteRunner,
              'profile': ProfileSuiteRunner}
