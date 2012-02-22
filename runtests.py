#!/usr/bin/env python
'''Stdnet asynchronous test suite. Requires pulsar or nose
'''
import sys
import os

from stdnet.conf import settings
from stdnet.utils import PPath
from stdnet import getdb

import tests
p = PPath(__file__)

pulsar = p.add(module = 'pulsar', up = 1, down = ('pulsar',))
if pulsar:    
    from pulsar.apps.test import TestSuite, TestOptionPlugin
    from pulsar.apps.test.plugins import bench

    
    class TestServer(TestOptionPlugin):
        name = "server"
        flags = ["-s", "--server"]
        desc = 'Backend server where to run tests.'
        default = settings.DEFAULT_BACKEND
        
        def configure(self, cfg):
            settings.DEFAULT_BACKEND = cfg.server
            settings.REDIS_PY_PARSER = cfg.http_py_parser
            settings.redis_status()
            
    
    class TestDataSize(TestOptionPlugin):
        name = "size"
        flags = ["--size"]
        desc = 'Size of the dataset to test. Choose one between "tiny", "small",\
     "normal", "big", "huge".'
        default = 'small'
            
if __name__ == '__main__':
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
    else:
        os.environ['stdnet_test_suite'] = 'nose'
        import nose
        nose.main()
