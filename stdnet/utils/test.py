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

    @classmethod
    def backend_params(cls):
        '''Optional backend parameters for tests in this class'''
        return {}
    
    @classmethod
    def setUpClass(cls):
        if not cls.models and cls.model:
            cls.models = (cls.model,)
        if not cls.model and cls.models:
            cls.model = cls.models[0]
        cls.namespace = 'stdnet-test-%s.' % gen_unique_id()
        cls.backend = getdb(namespace=cls.namespace, **cls.backend_params())
        if cls.backend.name == 'redis':
            r = cls.backend.client.script_flush()
            if isinstance(r, BackendRequest):
                return r.add_callback(lambda r: self.clear_all())
        return cls.clear_all()
    
    @classmethod
    def register(cls):
        '''Utility for registering the managers to the current backend.
This should be used with care in parallel testing. All registered models
will be unregistered after the :meth:`tearDown` method.'''
        for model in cls.models:
            odm.register(model, cls.backend)
    
    @classmethod
    def clear_all(cls):
        #override if you don't want to flush the database at the and of all
        #tests in this class
        return cls.backend.flush()

    def session(self, **kwargs):
        '''Create a new :class:`stdnet.odm.Session` bind to the
:attr:`TestCase.backend` attribute.'''
        session = odm.Session(self.backend, **kwargs)
        self.assertEqual(session.backend, self.backend)
        return session

    def load_scripts(self):
        if self.backend.name == 'redis':
            self.backend.load_scripts()
            
    def assertEqualId(self, instance, value, exact=False):
        pk = instance.pkvalue()
        if exact or self.backend.name == 'redis':
            self.assertEqual(pk, value)
        elif self.backend.name == 'mongo':
            if instance._meta.pk.type == 'auto':
                self.assertTrue(pk)
            else:
                self.assertEqual(pk, value)
        else:
            raise NotImplementedError()
        return pk
    
    def assertCommands(self, transaction):
        self.assertTrue(transaction.commands)
        


class CleanTestCase(TestCase):
    '''A test case which flush the database at every test.'''
    def _pre_setup(self):
        self.clear_all()
        
    def _post_teardown(self):
        if self.backend:
            self.clear_all()
            odm.unregister()
        
    def __call__(self, result=None):
        """Wrapper around default __call__ method
to perform cleanup, registration and unregistration.
        """
        self._pre_setup()
        super(CleanTestCase, self).__call__(result)
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