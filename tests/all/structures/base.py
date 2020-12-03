__test__ = False

from stdnet import InvalidTransaction, odm


class StructMixin(object):
    multipledb = "redis"
    structure = None
    name = None

    def create_one(self):
        """Create a structure and add few elements. Must return an instance
        of the :attr:`structure`."""
        raise NotImplementedError

    def empty(self):
        models = self.mapper
        l = models.register(self.structure())
        self.assertTrue(l.id)
        models.session().add(l)
        self.assertTrue(l.session is not None)
        return l

    def not_empty(self):
        models = self.mapper
        l = models.register(self.create_one())
        self.assertTrue(l.id)
        yield models.session().add(l)
        self.assertTrue(l.session is not None)
        yield l

    def test_no_session(self):
        l = self.create_one()
        self.assertFalse(l.session)
        self.assertTrue(l.id)
        session = self.mapper.session()
        self.assertRaises(InvalidTransaction, session.add, l)

    def test_meta(self):
        models = self.mapper
        l = models.register(self.create_one())
        self.assertTrue(l.id)
        session = models.session()
        with session.begin() as t:
            t.add(l)  # add the structure to the session
            self.assertEqual(l.session, session)
            self.assertEqual(l._meta.name, self.name)
            self.assertEqual(l._meta.model._model_type, "structure")
            # Structure have always the persistent flag set to True
            self.assertTrue(l.get_state().persistent)
            self.assertTrue(l in session)
            size = yield l.size()
            self.assertEqual(size, 0)
        yield t.on_result
        yield l

    def test_commit(self):
        l = yield self.test_meta()
        yield self.async.assertTrue(l.size())

    def test_delete(self):
        models = self.mapper
        l = models.register(self.create_one())
        self.assertTrue(l.id)
        session = models.session()
        yield session.add(l)
        yield self.async.assertTrue(l.size())
        yield session.delete(l)
        yield self.async.assertEqual(l.size(), 0)
        self.assertEqual(l.session, session)

    def test_empty(self):
        """Create an empty structure"""
        models = self.mapper
        l = models.register(self.structure())
        self.assertTrue(l.id)
        session = models.session()
        with session.begin() as t:
            t.add(l)
        yield t.on_result
        yield self.async.assertEqual(l.size(), 0)
        self.assertEqual(l.session, session)
