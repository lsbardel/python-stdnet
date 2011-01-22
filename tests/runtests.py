import logging
import os
import sys
import stdnet.contrib as contrib
from stdnet.test import TestSuiteRunner
from stdnet.utils.importlib import import_module 

logger = logging.getLogger()

# directories for testing
LIBRARY        = 'stdnet'
TEST_FOLDERS   = ('regression',)

CUR_DIR        = os.path.split(os.path.abspath(__file__))[0]
CONTRIB_DIR    = os.path.dirname(contrib.__file__)
ALL_TEST_PATHS = [os.path.join(CUR_DIR,td) for td in TEST_FOLDERS]
ALL_TEST_PATHS.append(CONTRIB_DIR) 
if CUR_DIR not in sys.path:
    sys.path.insert(0,CUR_DIR)

LOGGING_MAP = {1: logging.CRITICAL,
               2: logging.INFO,
               3: logging.DEBUG}


class Silence(logging.Handler):
    def emit(self, record):
        pass 


def get_tests():
    tests = []
    join  = os.path.join
    for dirpath in ALL_TEST_PATHS:
        loc = os.path.split(dirpath)[1]
        for d in os.listdir(dirpath):
            if os.path.isdir(join(dirpath,d)):
                tests.append((loc,d))
    return tests


def import_tests(tags):
    apptests = []
    for loc,app in get_tests():
        if tags and app not in tags:
            logger.debug("Skipping tests for %s" % app)
            continue
        logger.debug("Try to import tests for %s" % app)
        test_module = '{0}.{1}.tests'.format(loc,app)
        if loc == 'contrib':
            test_module = '{0}.{1}'.format(LIBRARY,test_module)
            
        try:
            mod = import_module(test_module)
        except ImportError, e:
            logger.debug("Could not import tests for %s: %s" % (test_module,e))
            continue
        
        logger.debug("Adding tests for %s" % app)
        apptests.append(mod)
    return apptests


def setup_logging(verbosity):
    level = LOGGING_MAP.get(verbosity,None)
    if level is None:
        logger.addHandler(Silence())
    else:
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(level)
        
        
def run(tags = None, itags = None, verbosity = 1):
    setup_logging(verbosity)
    modules = import_tests(tags)
    runner  = TestSuiteRunner(verbosity = verbosity, itags = itags)
    runner.run_tests(modules)
    