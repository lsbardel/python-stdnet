'''Test case classes and plugins for stdnet testing. Requires pulsar_.


TestCase
=================

.. autoclass:: TestCase
   :members:
   :member-order: bysource


DataGenerator
===================

.. autoclass:: DataGenerator
   :members:
   :member-order: bysource
   

.. _pulsar: https://pypi.python.org/pypi/pulsar
'''
import os
import sys
import logging

import pulsar
from pulsar.utils import events
from pulsar.apps.test import unittest, mock, TestSuite, TestPlugin, sequential

from stdnet import getdb, settings
from stdnet.utils import gen_unique_id

from .populate import populate

skipUnless = unittest.skipUnless
LOGGER = logging.getLogger('stdnet.test')


class DataGenerator(object):
    '''A generator of data. It must be initialised with the :attr:`size`
parameter obtained from the command line which is avaiable as a class
attribute in :class:`TestCase`.

.. attribute:: sizes

    A dictionary of sizes for this generator. It is a class attribute with
    the following entries: ``tiny``, ``small``, ``normal``, ``big``
    and ``huge``.
    
.. attribute:: size

    The actual size of the data to be generated. Obtained from the :attr:`sizes`
    and the input ``size`` code during initialisation.
'''
    sizes = {'tiny': 10,
             'small': 100,
             'normal': 1000,
             'big': 10000,
             'huge': 1000000}

    def __init__(self, size, sizes=None):
        self.sizes = sizes or self.sizes
        self.size_code = size
        self.size = self.sizes[size]
        self.generate()

    def generate(self):
        '''Called during initialisation to generate the data. ``kwargs``
are additional key-valued parameter passed during initialisation. Must
be implemented by subclasses.'''
        pass

    def create(self, test, use_transaction=True):
        pass

    def populate(self, datatype='string', size=None, **kwargs):
        '''A shortcut for the :func:`stdnet.utils.populate` function.
If ``size`` is not given, the :attr:`size` is used.'''
        size = size or self.size
        return populate(datatype, size, **kwargs)
    
    def random_string(self, min_len=5, max_len=30):
        '''Return a random string'''
        return populate('string', 1, min_len=min_len, max_len=max_len)[0]
    
    
def create_backend(self):
    from stdnet import odm
    self.namespace = 'stdnet-test-%s:' % gen_unique_id()
    if self.connection_string:
        server = getdb(self.connection_string, namespace=self.namespace,
                       **self.backend_params())
        self.backend = server
        yield server.flush()
        self.mapper = odm.Router(self.backend)
        for model in self.models:
            self.mapper.register(model)
    
        
class TestCase(unittest.TestCase):
    '''A :class:`unittest.TestCase` subclass for testing stdnet with
synchronous and asynchronous connections. It contains
several class methods for testing in a parallel test suite.

.. attribute:: multipledb

    class attribute which indicates which backend can run the test. There are
    several options:
    
    * ``multipledb = False`` The test case does not require a backend and
      only one :class:`TestCase` class is added to the test-suite regardless
      of which backend has been tested.
    * ``multipledb = True``, the default falue. Create as many :class:`TestCase`
      classes as the number of backend tested, each backend will run the tests.
    * ``multipledb = string, list, tuple``, Only those backend will run tests.
      
.. attribute:: backend

    A :class:`stdnet.BackendDataServer` for this
    :class:`TestCase` class. It is a class attribute which is different
    for each :class:`TestCase` class and it is created by the
    :meth:`setUpClass` method.
    
.. attribute:: data_cls

    A :class:`DataGenerator` class for creating data. The data is created
    during the :meth:`setUpClass` class method.
    
.. attribute:: data

    The :class:`DataGenerator` instance created from :attr:`data_cls`.
    
.. attribute:: model

    The default :class:`StdModel` for this test. A class attribute.
    
.. attribute:: models

    A tuple of models which can be registered by this test. The :attr:`model`
    is always the model at index 0 in :attr:`models`.
    
.. attribute:: mapper

    A :class:`stdnet.odm.Router` with all :attr:`models` registered with
    :attr:`backend`.
'''
    models = ()
    model = None
    connection_string = None
    backend = None
    sizes = None
    data_cls = DataGenerator

    @classmethod
    def backend_params(cls):
        '''Optional :attr:`backend` parameters for tests in this
:class:`TestCase` class.'''
        return {}
    
    @classmethod
    def setup_models(cls):
        if not cls.models and cls.model:
            cls.models = (cls.model,)
        if not cls.model and cls.models:
            cls.model = cls.models[0]
        cls.data = cls.data_cls(cls.cfg.size, cls.sizes)
    
    @classmethod
    def setUpClass(cls):
        '''Set up this :class:`TestCase` before test methods are run. here
is where a :attr:`backend` server instance is created and it is unique for this
:class:`TestCase` class. It create the :attr:`mapper`,
a :class:`stdnet.odm.Router` with all :attr:`models` registered.
There shouldn't be any reason to override this method, use :meth:`after_setup`
class method instead.'''
        cls.setup_models()
        yield create_backend(cls)
        yield cls.after_setup()
        
    @classmethod
    def after_setup(cls):
        '''This class method can be used to setup this :class:`TestCase` class
after the :meth:`setUpClass` was called. By default it does nothing.'''
        pass
    
    @classmethod
    def tearDownClass(cls):
        if cls.backend is not None:
            return cls.backend.flush()

    @classmethod
    def session(cls, **kwargs):
        '''Create a new :class:`stdnet.odm.Session` bind to the
:attr:`TestCase.backend` attribute.'''
        return cls.mapper.session()
    
    @classmethod
    def query(cls, model=None):
        '''Shortcut function to create a query for a model.'''
        return cls.session().query(model or cls.model)

    @classmethod
    def multi_async(cls, iterable, **kwargs):
        '''Treat ``iterable`` as a container of asynchronous results.'''
        from stdnet.utils import async
        return async.multi_async(iterable, **kwargs)
    
    def assertEqualId(self, instance, value, exact=False):
        '''Assert the value of a primary key in a backend agnostic way.
        
:param instance: the :class:`StdModel` to check the primary key ``value``.
:param value: the value of the id to check against.
:param exact: if ``True`` the exact value must be matched. For redis backend
    this parameter is not used.
'''
        pk = instance.pkvalue()
        if exact or self.backend.name == 'redis':
            self.assertEqual(pk, value)
        elif self.backend.name == 'mongo':
            if instance._meta.pk.type == 'auto':
                self.assertTrue(pk)
            else:
                self.assertEqual(pk, value)
        else:
            raise NotImplementedError
        return pk


class TestWrite(TestCase):
    '''A variant of :class:`TestCase` which clean the backend at each
test function. Useful when testing write operations.'''
    @classmethod
    def setUpClass(cls):
        cls.setup_models()
        return cls.after_setup()
    
    @classmethod
    def tearDownClass(cls):
        pass
    
    def _pre_setup(self):
        return create_backend(self)
    
    def _post_teardown(self):
        if self.backend is not None:
            return self.backend.flush()
        
    def session(self):
        return self.mapper.session()
    
    def query(self, model=None):
        '''Shortcut function to create a query for a model.'''
        return self.session().query(model or self.model)
        

class StdnetPlugin(TestPlugin):
    name = "server"
    flags = ["-s", "--server"]
    nargs = '*'
    desc = 'Back-end data server where to run tests.'
    default = [settings.DEFAULT_BACKEND]
    validator = pulsar.validate_list
    
    py_redis_parser = pulsar.Setting(
                        flags=['--py-redis-parser'],
                        desc='Run tests using the python redis parser rather '
                             'the C implementation.',
                        action="store_true",
                        default=False)
    
    sync = pulsar.Setting(flags=['--sync'],
                          desc='Switch off asynchronous bindings',
                          action="store_true",
                          default=False)
    
    def configure(self, cfg):
        if cfg.sync:
            settings.ASYNC_BINDINGS = False
        if cfg.py_redis_parser:
            settings.REDIS_PY_PARSER = True
        
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
