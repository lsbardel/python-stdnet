from stdnet import test, orm, getdb

from stdnet.conf import settings
from stdnet.utils import gen_unique_id

from examples.models import SimpleModel


class TestSession(test.TestCase):
    model = SimpleModel
        
    def testQueryMeta(self):
        session = self.session()
        self.assertEqual(len(session.new),0)
        self.assertEqual(len(session.modified),0)
        self.assertEqual(len(session.deleted),0)
        qs = session.query(SimpleModel)
        self.assertTrue(isinstance(qs,orm.Query))
    
    def testSimpleCreate(self):
        session = self.session()
        session.begin()
        m = SimpleModel(code='pluto',group='planet')
        session.add(m)
        self.assertTrue(m in session)
        self.assertEqual(len(session.new),1)
        self.assertEqual(len(session.modified),0)
        self.assertEqual(len(session.deleted),0)
        self.assertTrue(m in session.new)
        session.commit()
        self.assertEqual(m.id,1)
        
    def testSimpleFilter(self):
        session = self.session()
        with session.begin():
            session.add(SimpleModel(code='pluto',group='planet'))
            session.add(SimpleModel(code='venus',group='planet'))
            session.add(SimpleModel(code='sun',group='star'))
        query = session.query(SimpleModel)
        self.assertEqual(query.session,session)
        qs = query.filter(group = 'planet')
        self.assertFalse(qs.executed)
        self.assertEqual(qs.count(), 2)
        self.assertTrue(qs.executed)
        qs = query.filter(group = 'star')
        self.assertEqual(qs.count(), 1)
        qs = query.filter(group = 'bla')
        self.assertEqual(qs.count(), 0)
        
    