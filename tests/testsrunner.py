import logging
import os
import sys
from stdnet.conf import settings
import stdnet.contrib as contrib
from stdnet.test import TEST_TYPES
from stdnet.utils.importer import import_module


logger = logging.getLogger()

LOGGING_MAP = {1: logging.CRITICAL,
               2: logging.INFO,
               3: logging.DEBUG}

LIBRARY = 'stdnet'
CUR_DIR = os.path.split(os.path.abspath(__file__))[0]
CONTRIB_DIR  = os.path.dirname(contrib.__file__)
ALL_TEST_PATHS = (lambda test_type : os.path.join(CUR_DIR,test_type),
                  lambda test_type : CONTRIB_DIR) 


class Silence(logging.Handler):
    def emit(self, record):
        pass 


def get_tests(test_type):
    '''Gather tests type'''
    tests = []
    join  = os.path.join
    for dirpath in ALL_TEST_PATHS:
        dirpath = dirpath(test_type)
        loc = os.path.split(dirpath)[1]
        for d in os.listdir(dirpath):
            if os.path.isdir(join(dirpath,d)):
                yield (loc,d)


def import_tests(tags, test_type):
    for loc,app in get_tests(test_type):
        if tags and app not in tags:
            logger.debug("Skipping tests for %s" % app)
            continue
        logger.debug("Try to import tests for %s" % app)
        test_module = '{0}.{1}.tests'.format(loc,app)
        if loc == 'contrib':
            test_module = '{0}.{1}.{2}'.format(LIBRARY,test_module,test_type)
            
        try:
            mod = import_module(test_module)
        except ImportError as e:
            logger.debug("Could not import tests for %s: %s" % (test_module,e))
            continue
        
        logger.debug("Adding tests for %s" % app)
        yield mod


def setup_logging(verbosity):
    level = LOGGING_MAP.get(verbosity,None)
    if level is None:
        logger.addHandler(Silence())
    else:
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(level)
        


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
    modules = import_tests(tags, test_type)
    runner  = TestSuiteRunner(verbosity = verbosity, itags = itags)
    runner.run_tests(modules)
    settings.DEFAULT_BACKEND = std
    