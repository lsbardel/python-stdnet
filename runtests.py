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
                      * profile")
    parser.add_option("-f", "--fail",
                      action="store_false",
                      dest="can_fail",
                      default=True,
                      help="If set, the tests won't run if there is an import error in tests")
    parser.add_option("-s", "--server",
                      action="store",
                      dest="server",
                      default='',
                      help="Backend server where to run tests")
    parser.add_option("-i", "--include",
                      action="store",
                      dest="itags",
                      default='',
                      help="Include develepment tags, comma separated")
    return parser

    
def addpath():
    # add the tests directory to the Python Path
    p = os.path
    path = p.join(p.split(p.abspath(__file__))[0],'tests')
    if path not in sys.path:
        sys.path.insert(0, path)
    
addpath()

if __name__ == '__main__':
    options, tags = makeoptions().parse_args()
    from testsrunner import run
    server = options.server
    itags  = options.itags.replace(' ','')
    itags  = None if not itags else itags.split(',')
    run(tags,
        test_type = options.test_type,
        itags=itags,
        verbosity=options.verbosity,
        backend=options.server)