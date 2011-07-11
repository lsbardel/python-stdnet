#!/usr/bin/env python

import os
import sys
from optparse import OptionParser

import stdnet


def makeoptions():
    parser = OptionParser()
    parser.add_option("-v", "--verbosity",
                      type = int,
                      action="store",
                      dest="verbosity",
                      default=1,
                      help="Tests verbosity level, one of 0, 1, 2 or 3")
    parser.add_option("-t", "--type",
                      action="store",
                      dest="test_type",
                      default='regression',
                      help="Test type, possible choices are:\n\
                      * regression (default)\n\
                      * bench\n\
                      * profile\n\
                      * fuzzy")
    parser.add_option("-f", "--fail",
                      action="store_false",
                      dest="can_fail",
                      default=True,
                      help="If set, the tests won't run if there is\
 an import error in tests")
    parser.add_option("-s", "--server",
                      action="store",
                      dest="server",
                      default='',
                      help="Backend server where to run tests")
    parser.add_option("-p", "--parser",
                      action="store",
                      dest="parser",
                      default='',
                      help="Specify the python parser.")
    parser.add_option("-i", "--include",
                      action="store",
                      dest="itags",
                      default='',
                      help="Include develepment tags, comma separated")
    return parser

    
def addpath(test_type):
    # add the tests directory to the Python Path
    p = os.path
    CUR_DIR = os.path.split(os.path.abspath(__file__))[0]
    if CUR_DIR not in sys.path:
        sys.path.insert(0, CUR_DIR)
    
    from stdnet import contrib
    CONTRIB_DIR  = os.path.dirname(contrib.__file__)
    TEST_DIR = p.join(CUR_DIR,'tests')
    if TEST_DIR not in sys.path:
        sys.path.insert(0, TEST_DIR)

    return (lambda test_type : os.path.join(TEST_DIR,test_type),
            lambda test_type : CONTRIB_DIR)
    

if __name__ == '__main__':
    options, tags = makeoptions().parse_args()
    test_type = options.test_type
    paths = addpath(test_type)
    
    from stdnet.conf import settings
    from stdnet.test import run
    itags  = options.itags.replace(' ','')
    itags  = None if not itags else itags.split(',')
    settings.REDIS_PARSER = options.parser
    run(paths,
        tags = tags,
        library = 'stdnet',
        test_type = options.test_type,
        itags=itags,
        verbosity=options.verbosity,
        backend=options.server)

