__test__ = False

from stdnet import odm, InvalidTransaction


class StructMixin(object):
    multipledb = 'redis'
    structure = None
    name = None
    
    def createOne(self, session):
        '''Create a structure and add few elements. Must return an instance
of the :attr:`structure`.'''
        raise NotImplementedError
    
    def asserGroups(self, l):
        sm = l.session.model(l._meta)
        self.assertEqual(len(sm),1)
        self.assertTrue(l.session)
        self.assertTrue(l.state().persistent)
        self.assertFalse(l in sm.new)
        self.assertTrue(l in sm.dirty)
        self.assertTrue(l in sm.modified)
        self.assertFalse(l in sm.loaded)
        
    def testMeta(self):
        session = self.session()
        # start the transaction
        session.begin()
        l = self.createOne(session)
        self.assertTrue(l.id)
        self.assertEqual(l.instance, None)
        self.assertEqual(l.session, session)
        self.assertEqual(l._meta.name, self.name)
        self.assertEqual(l._meta.model._model_type, 'structure')
        #Structure have always the persistent flag set to True
        self.assertTrue(l.state().persistent)
        self.assertTrue(l in session)
        self.assertEqual(l.size(), 0)
        self.asserGroups(l)
        return l
    
    def testCommit(self):
        l = self.testMeta()
        l.session.commit()
        self.assertTrue(l.state().persistent)
        self.assertTrue(l.size())
        self.assertTrue(l in l.session)
        self.asserGroups(l)
        
    def testTransaction(self):
        session = self.session()
        with session.begin():
            l = self.createOne(session)
            # Trying to save within a section will throw an InvalidTransaction
            self.assertRaises(InvalidTransaction, l.save)
            # Same for delete
            self.assertRaises(InvalidTransaction, l.delete)
            self.assertTrue(l.state().persistent)
            self.asserGroups(l)
        self.assertTrue(l.size())
        self.assertTrue(l.state().persistent)
        
    def testDelete(self):
        session = self.session()
        with session.begin():
            s = self.createOne(session)
        self.asserGroups(s)
        self.assertTrue(s.size())
        s.delete()
        self.assertEqual(s.size(),0)
        self.assertNotEqual(s.session,None)
        self.assertFalse(s in session)
        
    def testEmpty(self):
        session = self.session()
        with session.begin():
            l = session.add(self.structure())
        self.asserGroups(l)
        self.assertEqual(l.size(),0)
        self.assertEqual(l.session,session)
        self.asserGroups(l)
        self.assertFalse(l.state().deleted)
