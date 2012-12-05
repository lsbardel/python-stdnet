#!/usr/bin/env python
'''Stdnet asynchronous test suite. Requires pulsar or nose'''
import sys
import os
## This is for dev environment with pulsar and dynts.
## If not available, some tests won't run
p = os.path
dir = p.dirname(p.dirname(p.abspath(__file__)))
try:
    import pulsar
except ImportError:
    pdir = p.join(dir, 'pulsar')
    if os.path.isdir(pdir):
        sys.path.append(pdir)
        import pulsar
#
try:
    import dynts
except ImportError:
    pdir = p.join(dir, 'dynts')
    if os.path.isdir(pdir):
        sys.path.append(pdir)
        try:
            import dynts
        except ImportError:
            pass

from stdnet import getdb
from stdnet.conf import settings
from stdnet.utils import test
from stdnet.utils.test import nose, pulsar


def noseoption(argv, *vals, **kwargs):
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
        from pulsar.apps.test import TestSuite
        from pulsar.apps.test.plugins import bench, profile
        
        os.environ['stdnet_test_suite'] = 'pulsar'
        suite = TestSuite(
                description='Stdnet Asynchronous test suite',
                    modules=('tests.py',),
                    plugins=(test.StdnetPlugin(),
                             bench.BenchMark(),
                             profile.Profile())
                  )
        suite.start()
    elif nose:
        from stdnet.test import NoseDataSizePlugin, NoseStdnetServer
        os.environ['stdnet_test_suite'] = 'nose'
        argv = list(sys.argv)
        noseoption(argv, '-w', value = 'tests/regression')
        noseoption(argv, '--all-modules')
        nose.main(argv=argv, addplugins=[NoseStdnetServer(),
                                         NoseDataSizePlugin()])
    else:
        print('To run tests you need either pulsar or nose.')
        exit(0)

if __name__ == '__main__':
    start()