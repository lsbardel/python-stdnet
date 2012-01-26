#!/usr/bin/env python
'''Stdnet asynchronous test suite. Requires pulsar_.

.. _pulsar: 
'''
from stdnet.conf import settings
from stdnet.utils import PPath
from stdnet import getdb
p = PPath(__file__)
p.add(module = 'pulsar', up = 1, down = ('pulsar',))

from pulsar.apps.test import TestSuite, TestOptionPlugin, ExitTest
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
    suite = TestSuite(description = 'Stdnet Asynchronous test suite',
                      modules = ('tests',),
                      plugins = (TestServer(),
                                 bench.BenchMark(),)
                      )
    suite.start()