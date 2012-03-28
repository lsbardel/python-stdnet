#!/usr/bin/env python
'''Stdnet asynchronous test suite. Requires pulsar or nose
'''
import sys
import os

from stdnet.conf import settings
from stdnet.utils import PPath
from stdnet import getdb

PPath(__file__).add(module = 'pulsar', up = 1, down = ('pulsar',))

from stdnet.test import nose, pulsar, TestServer, StdnetServer


def noseoption(argv,*vals,**kwargs):
    if vals:
        for val in vals:
            if val in argv:
                return
        argv.append(vals[0])
        value = kwargs.get('value')
        if value is not None:
            argv.append(value)
                        
def start():
    global pulsar
    argv = sys.argv
    if len(argv) > 1 and argv[1] == 'nose':
        pulsar = None
        sys.argv.pop(1)
    
    if pulsar:
        os.environ['stdnet_test_suite'] = 'pulsar'
        suite = TestSuite(
                description = 'Stdnet Asynchronous test suite',
                    modules = ('tests',),
                    plugins = (TestServer(),
                               bench.BenchMark(),)
                  )
        suite.start()
    elif nose:
        os.environ['stdnet_test_suite'] = 'nose'
        argv = list(sys.argv)
        noseoption(argv, '-w', value = 'tests/regression')
        noseoption(argv, '--all-modules')
        nose.main(argv=argv, addplugins=[StdnetServer()])
    else:
        print('To run tests you need either pulsar or nose.')
        exit(0)

if __name__ == '__main__':
    start()
    