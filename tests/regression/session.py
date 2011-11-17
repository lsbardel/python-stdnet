from stdnet import test, orm, getdb

from stdnet.conf import settings

from examples.models import SimpleModel


class TestSession(test.TestCase):
    
    def testMeta(self):
        backend = getdb(settings.DEFAULT_BACKEND, prefix = 'testsession.')
        self.assertEqual(backend.namespace,'testsession.')
        session = orm.Session(backend)
        self.assertEqual(session.backend,backend)
        qs = session.query(SimpleModel)
        self.assertTrue(isinstance(qs,orm.QuerySet))
        
    