from random import randint

from stdnet import test, orm, CommitException
from stdnet.utils import populate, zip, range

from examples.models import SimpleModel


class TestUnique(test.TestCase):
    model = SimpleModel
    
    def setUp(self):
        self.register()
        
    def testAddNew(self):
        session = self.session()
        m = session.add(self.model(code = 'me', group = 'bla'))
        self.assertEqual(m.id,1)
        # Try to create another one
        s = self.model(code = 'me', group = 'foo')
        self.assertRaises(CommitException, s.save)
        query = session.query(self.model)
        self.assertEqual(query.count(),1)
        m = query.get(code = 'me')
        self.assertEqual(m.id,1)
        self.assertEqual(m.group,'bla')
    
    def testChangeValue(self):
        session = self.session()
        query = session.query(self.model)
        m = session.add(self.model(code = 'me'))
        self.assertEqual(m.id,1)
        m = query.get(code = 'me')
        self.assertEqual(m.id,1)
        # Save with different code
        m.code = 'foo'
        m.save()
        m = query.get(code = 'foo')
        self.assertEqual(m.id,1)
        self.assertRaises(self.model.DoesNotExist, query.get, code = 'me')
        self.assertEqual(query.count(),1)
    