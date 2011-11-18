from stdnet import test, orm, getdb

from stdnet.conf import settings
from stdnet.utils import gen_unique_id

from examples.models import SimpleModel


class TestSession(test.TestCase):
    
    def session(self):
        prefix = gen_unique_id()+'.'
        backend = getdb(settings.DEFAULT_BACKEND, prefix = prefix)
        self.assertEqual(backend.namespace,prefix)
        session = orm.Session(backend)
        self.assertEqual(session.backend,backend)
        return session
        
    def testMeta(self):
        session = self.session()
        qs = session.query(SimpleModel)
        self.assertTrue(isinstance(qs,orm.QuerySet))
    
    def testCreate(self):
        session = self.session()
        obj = session.save(SimpleModel(code='pluto',group='planet'))
        self.assertEqual(obj.id,1)
        
    