'''Test case classes and plugins for stdnet testing. Requires pulsar_.


TestCase
=================

.. autoclass:: TestCase
   :members:
   :member-order: bysource


.. _pulsar: https://pypi.python.org/pypi/pulsar
'''
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

import pulsar
from pulsar import multi_async
from pulsar.utils import events
from pulsar.apps.test import TestSuite, TestPlugin, sequential

from stdnet import odm, getdb, settings
from stdnet.utils import gen_unique_id

skipUnless = unittest.skipUnless
LOGGER = logging.getLogger('stdnet.test')


class TestCase(unittest.TestCase):
    '''A :class:`unittest.TestCase` subclass for testing stdnet with
synchronous and asynchronous connections. It contains
several class methods for testing in a parallel test suite.

.. attribute:: backend

    A :class:`stdnet.BackendDataServer` for this
    :class:`TestCase` class. It is a class attribute which is different
    for each :class:`TestCase` class and it is created my the
    :meth:`setUpClass` method.
'''
    models = ()
    model = None
    connection_string = None
    backend = None

    @classmethod
    def backend_params(cls):
        '''Optional :attr:`backend` parameters for tests in this
:class:`TestCase` class.'''
        return {}
    
    @classmethod
    def setUpClass(cls):
        '''Set up this :class:`TestCase` before test methods are run.'''
        if not cls.models and cls.model:
            cls.models = (cls.model,)
        if not cls.model and cls.models:
            cls.model = cls.models[0]
        cls.namespace = 'stdnet-test-%s.' % gen_unique_id()
        if cls.connection_string:
            server = getdb(cls.connection_string, namespace=cls.namespace,
                           **cls.backend_params())
            cls.backend = server
            yield cls.clear_all()
        yield cls.after_setup()
        
    @classmethod
    def after_setup(cls):
        pass
    
    @classmethod
    def tearDownClass(cls):
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
        '''Invokes :meth:`stdnet.BackendDataServer.flush` method on
the :attr:`backend` attribute.'''
        if cls.backend is not None:
            return cls.backend.flush()

    @classmethod
    def session(cls, **kwargs):
        '''Create a new :class:`stdnet.odm.Session` bind to the
:attr:`TestCase.backend` attribute.'''
        return odm.Session(cls.backend, **kwargs)
    
    @classmethod
    def query(cls, model=None):
        '''Shortcut function to create a query for a model.'''
        return cls.session().query(model or cls.model)

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


class StdnetPlugin(TestPlugin):
    name = "server"
    flags = ["-s", "--server"]
    nargs = '*'
    desc = 'Back-end data server where to run tests.'
    default = [settings.DEFAULT_BACKEND]
    validator = pulsar.validate_list
    
    py_redis_parser = pulsar.Setting(
                        flags=['--py-redis-server'],
                        desc='Set the redis parser to be the pure '\
                              'Python implementation.',
                        action="store_true",
                        default=False)

    def on_start(self):
        servers = []
        names = set()
        for s in self.config.server:
            try:
                s = getdb(s)
                s.ping()
            except Exception:
                LOGGER.error('Could not obtain server %s' % s,
                             exc_info=True)
            else:
                if s.name not in names:
                    names.add(s.name)
                    servers.append(s.connection_string)
        if not servers:
            raise pulsar.HaltServer('No server available. BAILING OUT')
        settings.servers = servers
        if self.config.py_redis_parser:
            settings.REDIS_PY_PARSER = True
        
    def loadTestsFromTestCase(self, cls):
        cls.size = self.config.size
        
        
class testmaker(object):
    
    def __init__(self, test, name, server):
        self.test = test
        self.cls_name = '%s_%s' % (test.__name__, name)
        self.server = server
        
    def __call__(self):
        new_test = type(self.cls_name, (self.test,), {})
        new_test.connection_string = self.server
        return new_test
        
    
def create_tests(sender=None, tests=None, **kwargs):
    servers = getattr(settings, 'servers', None)
    if isinstance(sender, TestSuite) and servers:
        for tag, test in list(tests):
            tests.pop(0)
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
                        tests.append((tag, testmaker(test, name, server)))
            if toadd:
                tests.append((tag, test))

events.bind('tests', create_tests)
