__test__ = False

from stdnet import odm, InvalidTransaction


class StructMixin(object):
    multipledb = 'redis'
    structure = None
    name = None
    
    def create_one(self):
        '''Create a structure and add few elements. Must return an instance
of the :attr:`structure`.'''
        raise NotImplementedError
    
    def asserGroups(self, l):
        sm = l.session.model(l._meta)
        self.assertEqual(len(sm),1)
        self.assertTrue(l.session)
        self.assertTrue(l.get_state().persistent)
        self.assertFalse(l in sm.new)
        self.assertTrue(l in sm.dirty)
        self.assertTrue(l in sm.modified)
        self.assertFalse(l in sm.loaded)
        
    def test_meta(self):
        # start the transaction
        session = self.session()
        with session.begin() as t:
            l = t.add(self.create_one())
            self.assertTrue(l.id)
            self.assertEqual(l.instance, None)
            self.assertEqual(l.session, session)
            self.assertEqual(l._meta.name, self.name)
            self.assertEqual(l._meta.model._model_type, 'structure')
            #Structure have always the persistent flag set to True
            self.assertTrue(l.get_state().persistent)
            self.assertTrue(l in session)
            size = yield l.size()
            self.assertEqual(size, 0)
            self.asserGroups(l)
        yield t.on_result
        yield l
    
    def test_commit(self):
        l = yield self.test_meta()
        self.assertTrue(l.get_state().persistent)
        self.assertTrue(l.size())
        self.assertTrue(l in l.session)
        self.asserGroups(l)
        
    def test_transaction(self):
        session = self.session()
        with session.begin() as t:
            l = t.add(self.create_one())
            # Trying to save within a section will throw an InvalidTransaction
            self.assertRaises(InvalidTransaction, l.save)
            # Same for delete
            self.assertRaises(InvalidTransaction, l.delete)
            self.assertTrue(l.get_state().persistent)
            self.asserGroups(l)
        yield t.on_result
        self.assertTrue(l.size())
        self.assertTrue(l.get_state().persistent)
        
    def test_delete(self):
        session = self.session()
        with session.begin() as t:
            s = t.add(self.create_one())
        yield t.on_result
        self.asserGroups(s)
        yield self.async.assertTrue(s.size())
        yield s.delete()
        yield self.async.assertEqual(s.size(),0)
        self.assertNotEqual(s.session, None)
        self.assertFalse(s in session)
        
    def test_empty(self):
        '''Create an empty structure'''
        session = self.session()
        with session.begin() as t:
            l = t.add(self.structure())
        yield t.on_result
        self.asserGroups(l)
        yield self.async.assertEqual(l.size(), 0)
        self.assertEqual(l.session, session)
        self.asserGroups(l)
        self.assertFalse(l.get_state().deleted)
