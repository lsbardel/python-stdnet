'''Test case classes and plugins for stdnet'''
import os
import sys
import logging
from inspect import isclass
from datetime import timedelta

import sys

if sys.version_info >= (2,7):
    import unittest
else:   # pragma nocover
    try:
        import unittest2 as unittest
    except ImportError:
        print('To run tests in python 2.6 you need to install '\
              'the unittest2 package')
        exit(0)

if sys.version_info < (3,3):
    try:
        import mock
    except ImportError:
        print('The mock library is required to run tests.')
        exit(0)
else:
    from unittest import mock

from stdnet import odm, getdb, BackendRequest
from stdnet.conf import settings
from stdnet.utils import gen_unique_id

skipUnless = unittest.skipUnless


class TestCase(unittest.TestCase):
    '''A :class:`unittest.TestCase` subclass for testing stdnet. It contains
some utility functions for tesing in a parallel test suite.

.. attribute:: backend

    A :class:`stdnet.BackendDataServer` for the :class:`TestCase`.
    It is different for each instance and it is created just before
    :meth:`setUp` method is called.
'''
    models = ()
    model = None
    backend = None

    def backend_params(self):
        '''Optional backend parameters'''
        return {}

    def session(self, **kwargs):
        '''Create a new :class:`stdnet.odm.Session` bind to the
:attr:`TestCase.backend` attribute.'''
        session = odm.Session(self.backend, **kwargs)
        self.assertEqual(session.backend, self.backend)
        return session

    def register(self):
        '''Utility for registering the managers to the current backend.
This should be used with care in parallel testing. All registered models
will be unregistered after the :meth:`tearDown` method.'''
        for model in self.models:
            odm.register(model, self.backend)

    def clear_all(self):
        return self.backend.flush(pattern=self.prefix + '*')

    def _pre_setup(self):
        if not self.models and self.model:
            self.models = (self.model,)
        if not self.model and self.models:
            self.model = self.models[0]
        self.prefix = 'stdnet-test-'+gen_unique_id()+'.'
        self.backend = getdb(prefix=self.prefix, **self.backend_params())
        r = None
        if self.backend.name == 'redis':
            r = self.backend.client.script_flush()
        if isinstance(r, BackendRequest):
            return r.add_callback(lambda r: self.clear_all())
        else:
            return self.clear_all()

    def load_scripts(self):
        if self.backend.name == 'redis':
            self.backend.load_scripts()

    def _post_teardown(self):
        if self.backend:
            session = odm.Session(self.backend)
            self.clear_all()
            odm.unregister()

    def __call__(self, result=None):
        """Wrapper around default __call__ method
to perform cleanup, registration and unregistration.
        """
        self._pre_setup()
        super(TestCase, self).__call__(result)
        self._post_teardown()


class DataSizePlugin(object):   # pragma nocover

    def configure(self, cfg, *args):
        self.enabled = True
        self.size = cfg.size

    def loadTestsFromTestCase(self, cls):
        cls.size = self.size


################################################################################
##    PULSAR PLUGINS
################################################################################
try:    # pragma nocover
    import pulsar
    from pulsar.apps.test import TestOptionPlugin
    from pulsar.apps.test.plugins import bench


    class PulsarStdnetServer(TestOptionPlugin):
        name = "server"
        flags = ["-s", "--server"]
        desc = 'Back-end data  server where to run tests.'
        default = settings.DEFAULT_BACKEND

        def configure(self, cfg):
            settings.DEFAULT_BACKEND = cfg.server
            settings.REDIS_PY_PARSER = cfg.http_py_parser


    class PulsarRedisParser(TestOptionPlugin):
        name = "py_redis_parser"
        flags = ["--py-redis-parser"]
        desc = 'Set the redis parser to be the pure Python implementation.'
        action = "store_true"
        default = False

        def configure(self, cfg):
            if cfg.py_redis_parser:
                self.REDIS_PY_PARSER = True


    class PulsarDataSizePlugin(DataSizePlugin, TestOptionPlugin):
        name = "size"
        flags = ["--size"]
        desc = 'Size of the dataset to test. Choose one between "tiny", '\
               '"small", "normal", "big", "huge".'
        default = 'small'

except ImportError: # pragma nocover
    pulsar = None


################################################################################
##    NOSE PLUGINS
################################################################################
try:    # pragma nocover
    import nose
    from nose import plugins

    class NoseStdnetServer(plugins.Plugin):

        def options(self, parser, env=os.environ):
            parser.add_option('--server',
                          dest='server',
                          default='',
                          help="Backend server where to run tests [{0}]"\
                                    .format(settings.DEFAULT_BACKEND))

        def configure(self, options, conf):
            self.enabled = True
            if options.server:
                settings.DEFAULT_BACKEND = options.server

    class NoseDataSizePlugin(DataSizePlugin, plugins.Plugin):

        def options(self, parser, env=os.environ):
            parser.add_option(
                          '--size',
                          dest='size',
                          default='small',
                          help='Size of the dataset to test. Choose one between\
 "tiny", "small", "normal", "big", "huge".')

except ImportError: # pragma nocover
    nose = None
    NoseStdnetServer = None
    NoseDataSizePlugin = None