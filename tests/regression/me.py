from stdnet import test
from stdnet.conf import settings
from stdnet.lib import redis
import stdnet as me


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
            