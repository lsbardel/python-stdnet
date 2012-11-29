from stdnet import odm, getdb

from stdnet.utils import test
from stdnet.conf import settings
from stdnet.utils import gen_unique_id

from examples.models import SimpleModel, Instrument


class TestSession(test.CleanTestCase):
    model = SimpleModel
        
    def testQueryMeta(self):
        session = self.session()
        self.assertEqual(len(session._models),0)
        qs = session.query(SimpleModel)
        self.assertTrue(isinstance(qs,odm.Query))
    
    def testSimpleCreate(self):
        session = self.session()
        session.begin()
        m = SimpleModel(code='pluto', group='planet')
        session.add(m)
        self.assertTrue(m in session)
        sm = session.model(m._meta)
        self.assertEqual(len(sm.new),1)
        self.assertEqual(len(sm.modified),0)
        self.assertEqual(len(sm.deleted),0)
        self.assertTrue(m in sm.new)
        session.commit()
        self.assertEqual(m.id,1)
        
    def testCreate2Models(self):
        # Tests a session with two models. This was for a bug
        session = self.session()
        with session.begin():
            session.add(SimpleModel(code='pluto',group='planet'))
            session.add(Instrument(name='bla',ccy='EUR',type='equity'))
        self.assertEqual(session.query(SimpleModel).count(), 1)
        self.assertEqual(session.query(Instrument).count(), 1)
        
    def testSimpleFilter(self):
        session = self.session()
        with session.begin():
            session.add(SimpleModel(code='pluto',group='planet'))
            session.add(SimpleModel(code='venus',group='planet'))
            session.add(SimpleModel(code='sun',group='star'))
        query = session.query(SimpleModel)
        self.assertEqual(query.count(),3)
        self.assertEqual(query.session,session)
        all = query.all()
        self.assertEqual(len(all),3)
        qs = query.filter(group = 'planet')
        self.assertFalse(qs.executed)
        self.assertEqual(qs.count(), 2)
        self.assertTrue(qs.executed)
        qs = query.filter(group = 'star')
        self.assertEqual(qs.count(), 1)
        qs = query.filter(group = 'bla')
        self.assertEqual(qs.count(), 0)
        
    def testModifyIndexField(self):
        session = self.session()
        with session.begin():
            session.add(SimpleModel(code='pluto', group='planet'))
        query = session.query(SimpleModel)
        qs = query.filter(group='planet')
        self.assertEqual(qs.count(), 1)
        el = qs[0]
        self.assertEqual(el.id, 1)
        session = self.session()
        el.group = 'smallplanet'
        with session.begin():
            session.add(el)
        self.assertEqual(el.id, 1)
        # lets get it from the server
        qs = session.query(self.model).filter(id=1)
        self.assertEqual(qs.count(), 1)
        el = qs[0]
        self.assertEqual(el.code, 'pluto')
        self.assertEqual(el.group, 'smallplanet')
        # now filter on group
        qs = session.query(self.model).filter(group='smallplanet')
        self.assertEqual(qs.count(), 1)
        self.assertEqual(qs[0].id, 1)
        # now filter on old group
        qs = session.query(self.model).filter(group='planet')
        self.assertEqual(qs.count(), 0)
    