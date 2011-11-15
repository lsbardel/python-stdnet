from stdnet import test, orm, getdb

from stdnet.conf import settings

from examples.models import SimpleModel


class TestSession(test.TestCase):
    
    def testMeta(self):
        backend = getdb(settings.DEFAULT_BACKEND, prefix = 'testsession.')
        self.assertEqual(backend.namespace,'testsession.')
        s = orm.Session(backend)
        self.assertEqual(s.backend,backend)
    