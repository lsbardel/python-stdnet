from stdnet import odm, getdb, BackendDataServer, ModelNotAvailable,\
                    SessionNotAvailable, BackendStructure
from stdnet.utils import test

from examples.models import SimpleModel


class DummyBackendDataServer(BackendDataServer):
    default_port = 9090
    def setup_connection(self, address):
        pass


class TestBackend(test.TestCase):
    multipledb = False

    def get_backend(self, **kwargs):
        return DummyBackendDataServer(**kwargs)

    def testVirtuals(self):
        self.assertRaises(NotImplementedError, BackendDataServer, '', '')
        b = self.get_backend()
        self.assertEqual(str(b), 'dummy://127.0.0.1:9090')
        self.assertFalse(b.clean(None))
        self.assertRaises(NotImplementedError, b.execute_session, None, None)
        self.assertRaises(NotImplementedError, b.model_keys, None)
        self.assertRaises(NotImplementedError, b.flush)

    def testMissingStructure(self):
        l = odm.List()
        self.assertRaises(AttributeError, l.backend_structure)

    def testRedis(self):
        b = getdb('redis://')
        self.assertEqual(b.name, 'redis')
        self.assertEqual(b.connection_string, 'redis://127.0.0.1:6379?db=0')

    def testBackendStructure_error(self):
        s = BackendStructure(None, None, None)
        self.assertRaises(NotImplementedError, s.flush)
        self.assertRaises(NotImplementedError, s.delete)
        self.assertRaises(NotImplementedError, s.size)
