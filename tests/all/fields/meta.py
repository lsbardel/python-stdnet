'''Field metadata and full coverage.'''
import stdnet
from stdnet import odm, FieldError
from stdnet.utils import test


class TestFields(test.TestCase):
    
    def testBaseClass(self):
        self.assertRaises(TypeError, odm.Field, kaputt=True)
        f = odm.Field()
        self.assertEqual(f.to_python(self), self)
        f = odm.StructureField()
        self.assertEqual(f.model, None)
        self.assertEqual(f.to_python(self), None)
        self.assertRaises(NotImplementedError, f.structure_class)
        
    def testDoublePK(self):
        def bad_class():
            class MyBadClass(odm.StdModel):
                id = odm.IntegerField(primary_key=True)
                code = odm.SymbolField(primary_key=True)
        self.assertRaises(FieldError, bad_class)
        