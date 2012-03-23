'''Field metadata and full coverage.'''
import stdnet
from stdnet import test, orm, FieldError

from examples.models import Task, WordBook, SimpleModel


def genid():
    return str(uuid4())[:8]


class TestFields(test.TestCase):
    
    def testBaseClass(self):
        self.assertRaises(TypeError, orm.Field, kaputt = True)
        f = orm.Field()
        self.assertEqual(f.to_python(self), self)
        f = orm.StructureField()
        self.assertEqual(f.model, None)
        self.assertEqual(f.to_python(self), None)
        self.assertRaises(NotImplementedError, f.structure_class)
        
    def testDoublePK(self):
        def bad_class():
            class MyBadClass(orm.StdModel):
                id = orm.IntegerField(primary_key = True)
                code = orm.SymbolField(primary_key = True)
        self.assertRaises(FieldError, bad_class)
        