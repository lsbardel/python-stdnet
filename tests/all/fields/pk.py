import stdnet
from stdnet import odm, FieldError
from stdnet.utils import test

from examples.models import Parent, Child


class TestForeignKey(test.TestCase):
    models = (Parent, Child)

    def test_custom_pk(self):
        models = self.mapper
        parent = yield models.parent.new(name='test')
        self.assertEqual(parent.pkvalue(), 'test')
        self.assertEqual(parent.pk().name, 'name')

    def test_add_parent_and_child(self):
        models = self.mapper
        with models.session().begin() as t:
            parent = models.parent(name='test2')
            child = models.child(parent=parent, name='foo')
            self.assertEqual(child.parent, parent)
            self.assertEqual(child.parent_id, parent.pkvalue())
            t.add(parent)
            t.add(child)
        yield t.on_result


class TestQuery(test.TestCase):
    models = (Parent, Child)

    def test_non_id_pk(self):
        '''
        Models with non-'id' primary keys should be queryable (regression test)

        '''
        models = self.mapper
        with models.session().begin() as t:
            parent = models.parent(name='test2')
            child = models.child(parent=parent, name='foo')
            t.add(parent)
            t.add(child)
        yield t.on_result
        with models.session().begin() as t:
            parents = t.query(Parent).all()
            self.assertEqual(len(parents), 1)
        yield t.on_result


class TestManyToMany(test.TestCase):
    models = (Parent, Child)

    def test_non_id_pk(self):
        '''
        Models with non-'id' primary keys should be queryable from a ManyToMany
        relation (regression test)

        '''
        models = self.mapper
        with models.session().begin() as t:
            parent = models.parent(name='test2')
            uncle = models.parent(name='test3')
            child = models.child(parent=parent, name='foo')
            t.add(parent)
            t.add(uncle)
            t.add(child)
        yield t.on_result
        with models.session().begin() as t:
            child.uncles.add(uncle)
            self.assertEqual(len(child.uncles.all()), 1)
        yield t.on_result
