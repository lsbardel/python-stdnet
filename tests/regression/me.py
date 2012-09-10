from stdnet import test, BackendRequest, BackendStructure
from stdnet.conf import settings
from stdnet.lib import redis
import stdnet as me

from examples.models import SimpleModel

class TestInitFile(test.TestCase):

    def test_version(self):
        self.assertTrue(len(me.VERSION), 5)
        version = me.__version__
        self.assertTrue(version)
        self.assertEqual(me.__version__,me.get_version(me.VERSION))
        
    def testStdnetVersion(self):
        self.assertRaises(TypeError, me.stdnet_version, 1,2,3,4,5)

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__", "__doc__"):
            self.assertTrue(getattr(me, m, None))
            
            
class TestBackendClasses(test.TestCase):
    
    def testBackendRequest(self):
        b = BackendRequest()
        self.assertRaises(NotImplementedError, b.add_callback, None)
        
    def testBackendStructure_error(self):
        m = SimpleModel()
        self.assertRaises(ValueError, BackendStructure, m, None, None)
