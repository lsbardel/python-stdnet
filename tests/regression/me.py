from stdnet import test
from stdnet.conf import settings
from stdnet.lib.redis import ConnectionError
import stdnet as me


class TestInitFile(test.TestCase):

    def test_version(self):
        self.assertTrue(len(me.VERSION), 5)
        version = me.__version__
        self.assertTrue(version)
        self.assertEqual(me.__version__,me.get_version(me.VERSION))

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__", "__doc__"):
            self.assertTrue(getattr(me, m, None))
            
    def testSettings(self):
        db = settings.DEFAULT_BACKEND
        try:
            settings.DEFAULT_BACKEND = 'redis://dksnkdcnskcnskcn:6379?db=7'
            self.assertRaises(ConnectionError, settings.redis_status)
        finally:
            settings.DEFAULT_BACKEND = db