from stdnet.utils import test

from examples.models import Environment



class TestPickleObjectField(test.TestCase):
    model = Environment
    
    def testMetaData(self):
        field = self.model._meta.dfields['data']
        self.assertEqual(field.type,'object')
        self.assertEqual(field.internal_type,'bytes')
        self.assertEqual(field.index,False)
        self.assertEqual(field.name,field.attname)
        return field
    
    def testOkObject(self):
        session = self.session()
        v = self.model(data=['ciao','pippo'])
        self.assertEqual(v.data, ['ciao','pippo'])
        yield session.add(v)
        self.assertEqual(v.data, ['ciao','pippo'])
        v = yield session.query(self.model).get(id=v.id)
        self.assertEqual(v.data, ['ciao','pippo'])
        
    def testRecursive(self):
        '''Silly test to test both pickle field and picklable instance'''
        session = self.session()
        v = yield session.add(self.model(data=('ciao','pippo', 4, {})))
        v2 = self.model(data=v)
        self.assertEqual(v2.data, v)
        yield session.add(v2)
        self.assertEqual(v2.data, v)
        v2 = yield session.query(self.model).get(id=v2.id)
        self.assertEqual(v2.data, v)