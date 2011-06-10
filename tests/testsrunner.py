import logging
import os
import sys

from stdnet.conf import settings
import stdnet.contrib as contrib
from stdnet.test import TEST_TYPES, setup_logging, import_tests


LIBRARY = 'stdnet'
CUR_DIR = os.path.split(os.path.abspath(__file__))[0]
CONTRIB_DIR  = os.path.dirname(contrib.__file__)
ALL_TEST_PATHS = (lambda test_type : os.path.join(CUR_DIR,test_type),
                  lambda test_type : CONTRIB_DIR)

if CONTRIB_DIR not in sys.path:
    sys.path.insert(0,CONTRIB_DIR)


def run(tags = None, test_type = None,
        itags = None, verbosity = 1, backend = None):
    '''Test Runner'''
    if CONTRIB_DIR not in sys.path:
        sys.path.insert(0,CONTRIB_DIR)
    std = settings.DEFAULT_BACKEND
    settings.DEFAULT_BACKEND =  backend or 'redis://127.0.0.1:6379/?db=13'
    setup_logging(verbosity)
    test_type = test_type or 'regression'
    if test_type not in TEST_TYPES:
        print(('Unknown test type {0}. Must be one of {1}.'.format(test_type, ', '.join(TEST_TYPES))))
        exit()
    TestSuiteRunner = TEST_TYPES[test_type]
    if not TestSuiteRunner:
        print(('No test suite for {0}'.format(test_type)))
        exit()
    modules = import_tests(tags, test_type, ALL_TEST_PATHS, LIBRARY)
    runner  = TestSuiteRunner(verbosity = verbosity, itags = itags)
    runner.run_tests(modules)
    settings.DEFAULT_BACKEND = std
    