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