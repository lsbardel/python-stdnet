from stdnet import test, BackendDataServer, ModelNotAvailable,\
                     SessionNotAvailable
from stdnet import odm


class DummyBackendDataServer(BackendDataServer):
    
    def setup_connection(self, address):
        pass


class TestBackend(test.TestCase):
    
    def get_backend(self, name='?', address=('',0)):
        return DummyBackendDataServer(name, address)
    
    def testVirtuals(self):
        self.assertRaises(NotImplementedError, BackendDataServer, '', '')
        b = self.get_backend()
        self.assertEqual(str(b), '')
        self.assertFalse(b.clean(None))
        self.assertRaises(NotImplementedError, b.execute_session, None, None)
        self.assertRaises(NotImplementedError, b.model_keys, None)
        self.assertRaises(NotImplementedError, b.instance_keys, None)
        self.assertRaises(NotImplementedError, b.as_cache)
        self.assertRaises(NotImplementedError, b.clear)
        self.assertRaises(NotImplementedError, b.flush)
        self.assertRaises(NotImplementedError, b.publish, '', '')
        
    def testMissingStructure(self):
        l = odm.List()
        self.assertRaises(SessionNotAvailable, l.backend_structure)
        session = odm.Session(backend=self.get_backend())
        session.begin()
        session.add(l)
        self.assertRaises(ModelNotAvailable, l.backend_structure)

        
        
        