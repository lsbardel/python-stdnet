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
        
    def testQueryMeta(self):
        session = self.session()
        qs = session.query(SimpleModel)
        self.assertTrue(isinstance(qs,orm.Query))
    
    def testCreate(self):
        session = self.session()
        session.begin()
        m = SimpleModel(code='pluto',group='planet')
        session.add(m)
        self.assertTrue(m in session)
        session.commit()
        self.assertEqual(m.id,1)
        
    def testCreateTransaction(self):
        session = SimpleModel.objects.session()
        with session.begin():
            session.add(SimpleModel(code='pluto',group='planet'))
            session.add(SimpleModel(code='venus',group='planet'))
            session.add(SimpleModel(code='sun',group='star'))
        query = session.query(SimpleModel)
        self.assertEqual(query.session,session)
        qs = query.filter(group = 'planet')
        self.assertEqual(qs.count(),2)
        
    