#!/usr/bin/env python
import os
import sys
import argparse

import stdnet
from stdnet import contrib
from stdnet.test import run
from stdnet.conf import settings


def makeoptions():
    parser = argparse.ArgumentParser(description='______ STDNET TEST SUITE',
                                     epilog="Have fun!")
    parser.add_argument('labels',nargs='*',
                        help='Optional test labels to run. If not provided\
 all tests are run. To see available labels use the -l option.')
    parser.add_argument('-v', '--verbosity', type=int, dest='verbosity',
                        default=1, action='store',
                        help='Tests verbosity level, one of 0, 1, 2 or 3')
    parser.add_argument('-t', '--type', dest='test_type',
                        default='regression', action='store',
                        help='Test type, possible choices are:\
 regression (default), bench, profile, fuzzy.')
    parser.add_argument('-f', '--fail', action="store_false",
                        dest="can_fail", default=True,
                        help="If set, tests will run even if there is\
 an import error in tests.")
    parser.add_argument("-s", "--server", action="store",
                        dest="server", default='',
                        help='Backend server where to run tests. By default\
 tests are run on "{0}"'.format(settings.DEFAULT_BACKEND))
    parser.add_argument("-l", "--list", action="store_true",
                        dest="list_labels", default=False,
                        help="List all test labels without performing tests.")
    parser.add_argument("-p", "--parser", action="store",
                        dest="parser", default='',
                        help="Specify the python parser.")
    parser.add_argument("-i", "--include", action="store",
                        dest="itags", default='', nargs='*',
                        help="Include develepment tags, comma separated")
    return parser

    
def addpath(test_type):
    # add the tests directory to the Python Path
    p = os.path
    CUR_DIR = os.path.split(os.path.abspath(__file__))[0]
    if CUR_DIR not in sys.path:
        sys.path.insert(0, CUR_DIR)
    
    CONTRIB_DIR  = os.path.dirname(contrib.__file__)
    TEST_DIR = p.join(CUR_DIR,'tests')
    if TEST_DIR not in sys.path:
        sys.path.insert(0, TEST_DIR)

    return (lambda test_type : os.path.join(TEST_DIR,test_type),
            lambda test_type : CONTRIB_DIR)
    

if __name__ == '__main__':
    options = makeoptions().parse_args()
    paths = addpath(options.test_type)
    settings.REDIS_PARSER = options.parser
    
    run(paths,
        tags = options.labels,
        library = 'stdnet',
        test_type = options.test_type,
        itags=options.itags,
        verbosity=options.verbosity,
        backend=options.server,
        list_labels=options.list_labels,
        can_fail=options.can_fail)

