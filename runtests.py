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

    
if __name__ == '__main__':
    options, tags = makeoptions().parse_args()
    server = options.server
    itags  = options.itags.replace(' ','')
    itags  = None if not itags else itags.split(',')
    stdnet.runtests(tags,
                    itags=itags,
                    verbosity=options.verbosity,
                    backend=options.server)