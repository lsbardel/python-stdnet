#!/usr/bin/env python
'''Stdnet asynchronous test suite. Requires pulsar.'''
import sys
import os
from multiprocessing import current_process

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
from pulsar.apps.test import TestSuite
from pulsar.apps.test.plugins import bench, profile
from pulsar.utils.path import Path
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


def run(**params):
    args = params.get('argv', sys.argv)
    if '--coverage' in args or params.get('coverage'):
        import coverage
        p = current_process()
        p._coverage = coverage.coverage(data_suffix=True)
        p._coverage.start()
    runtests(**params)


def runtests(**params):
    import stdnet
    from stdnet.utils import test
    #
    strip_dirs = [Path(stdnet.__file__).parent.parent, os.getcwd()]
    #
    suite = TestSuite(description='Stdnet Asynchronous test suite',
                      modules=('tests.all',),
                      plugins=(test.StdnetPlugin(),
                               bench.BenchMark(),
                               profile.Profile()),
                      **params)
    #suite.bind_event('tests', test.create_tests)
    suite.start()
    #
    if suite.cfg.coveralls:
        from pulsar.utils.cov import coveralls
        coveralls(strip_dirs=strip_dirs,
                  stream=suite.stream,
                  repo_token='ZQinNe5XNbzQ44xYGTljP8R89jrQ5xTKB')


if __name__ == '__main__':
    run()
