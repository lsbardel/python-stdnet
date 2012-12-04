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
logger = logging.getLogger('stdnet.test')

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
    connection_string = None
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
        if cls.connection_string:
            server = getdb(cls.connection_string, namespace=cls.namespace,
                           **cls.backend_params())
            cls.backend = server
            if server.name == 'redis':
                r = server.client.script_flush()
                if isinstance(r, BackendRequest):
                    return r.add_callback(lambda r: cls.clear_all())
            return cls.clear_all()
        
    @classmethod
    def load_scripts(cls):
        if cls.backend and cls.backend.name == 'redis':
            cls.backend.load_scripts()
    
    @classmethod
    def register(cls):
        '''Utility for registering the managers to the current backend.
This should be used with care in parallel testing. All registered models
will be unregistered after the :meth:`tearDown` method.'''
        if cls.backend:
            for model in cls.models:
                odm.register(model, cls.backend)
    
    @classmethod
    def clear_all(cls):
        #override if you don't want to flush the database at the and of all
        #tests in this class
        if cls.backend is not None:
            yield cls.backend.flush()

    @classmethod
    def session(cls, **kwargs):
        '''Create a new :class:`stdnet.odm.Session` bind to the
:attr:`TestCase.backend` attribute.'''
        return odm.Session(cls.backend, **kwargs)

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
        return self.clear_all()
        
    def _post_teardown(self):
        if self.backend:
            yield self.clear_all()
            yield odm.unregister()
        
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
    from pulsar.utils import event
    from pulsar.apps.test import TestSuite, TestOptionPlugin
    from pulsar.apps.test.plugins import bench


    class PulsarStdnetServer(TestOptionPlugin):
        name = "server"
        flags = ["-s", "--server"]
        nargs = '*'
        desc = 'Back-end data  server where to run tests.'
        default = [settings.DEFAULT_BACKEND]
        validator = pulsar.validate_list

        def on_start(self):
            servers = []
            names = set()
            for s in self.value:
                try:
                    s = getdb(s)
                    s.ping()
                except:
                    logger.error('Could not obtain server %s' % s,
                                 exc_info=True)
                else:
                    if s.name not in names:
                        names.add(s.name)
                        servers.append(s.connection_string)
            settings.servers = servers


    class PulsarRedisParser(TestOptionPlugin):
        name = "py_redis_parser"
        flags = ["--py-redis-parser"]
        desc = 'Set the redis parser to be the pure Python implementation.'
        action = "store_true"
        default = False

        def on_start(self):
            if self.value:
                settings.REDIS_PY_PARSER = True


    class PulsarDataSizePlugin(DataSizePlugin, TestOptionPlugin):
        name = "size"
        flags = ["--size"]
        desc = 'Size of the dataset to test. Choose one between "tiny", '\
               '"small", "normal", "big", "huge".'
        default = 'small'
            
            
    class testmaker(object):
        
        def __init__(self, test, name, server):
            self.test = test
            self.cls_name = '%s_%s' % (test.__name__, name)
            self.server = server
            
        def __call__(self):
            new_test = type(self.cls_name, (self.test,), {})
            new_test.connection_string = self.server
            return new_test
            
        
    def create_tests(sender=None, value=None):
        servers = getattr(settings, 'servers', None)
        if isinstance(sender, TestSuite) and servers:
            for tag, test in list(value):
                value.pop(0)
                multipledb = getattr(test, 'multipledb', True)
                toadd = True
                if isinstance(multipledb, str):
                    multipledb = [multipledb]
                if isinstance(multipledb, (list, tuple)):
                    toadd = False
                if multipledb:
                    for server in servers:
                        name = server.split('://')[0]
                        if multipledb == True or name in multipledb:
                            toadd = False
                            value.append((tag, testmaker(test, name, server)))
                if toadd:
                    value.append((tag, test))
    
    event.bind('tests', create_tests)

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