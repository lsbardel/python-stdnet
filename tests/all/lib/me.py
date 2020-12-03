import stdnet as me
from stdnet import settings
from stdnet.utils import test


class TestInitFile(test.TestCase):
    multipledb = False

    def test_version(self):
        self.assertTrue(len(me.VERSION), 5)
        version = me.__version__
        self.assertTrue(version)
        self.assertEqual(me.__version__, me.get_version(me.VERSION))

    def testStdnetVersion(self):
        self.assertRaises(TypeError, me.stdnet_version, 1, 2, 3, 4, 5)

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__", "__doc__"):
            self.assertTrue(getattr(me, m, None))
