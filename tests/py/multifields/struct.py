__test__ = False
from time import sleep

from stdnet import StructureFieldError
from stdnet.utils import test, populate, zip, to_string


class StringData(test.DataGenerator):
    
    def generate(self):
        self.names = self.populate()
        

class MultiFieldMixin(object):
    '''Test class which add a couple of tests for multi fields.'''
    attrname = 'data'
    data_cls = StringData
        
    def setUp(self):
        self.names = test.populate('string', size=10)
        self.name = self.names[0]
        
    def defaults(self):
        return {}
        
    def get_object_and_field(self, save=True, **kwargs):
        models = self.mapper
        params = self.defaults()
        params.update(kwargs)
        m = self.model(**params)
        if save:
            yield models.session().add(m)
        yield m, getattr(m, self.attrname)
    
    def adddata(self, obj):
        raise NotImplementedError
    
    def test_RaiseStructFieldError(self):
        yield self.async.assertRaises(StructureFieldError,
                                      self.get_object_and_field, False)
    
    def test_multi_field_meta(self):
        '''Here we check for multifield specific stuff like the instance
related keys (keys which are related to the instance rather than the model).'''
        # get instance and field, the field has no data here
        models = self.mapper
        #
        obj, field = yield self.get_object_and_field()
        #
        self.assertTrue(field.field)
        self.assertEqual(field.field.model, self.model)
        self.assertEqual(field._pkvalue, obj.pkvalue())
        self.assertEqual(field.session, obj.session)
        #
        be = field.backend_structure()
        self.assertEqual(be.backend, models[self.model].backend)
        self.assertEqual(be.instance, field)
        #
        if be.backend.name == 'redis':
            yield self.check_redis_structure(obj, be)
        
    def check_redis_structure(self, obj, be):
        session = obj.session
        backend = be.backend
        #
        # field id should be in instance keys
        keys = backend.instance_keys(obj)
        self.assertTrue(be.id in keys)
        #
        # the field has no data, so there is no key in the database
        lkeys = yield backend.model_keys(self.model._meta)
        self.assertFalse(be.id in lkeys)
        #
        # Lets add data
        yield self.adddata(obj)
        # The field id should be in the server keys
        if backend.name == 'redis':
            lkeys = yield backend.model_keys(self.model._meta)
            self.assertTrue(be.id in lkeys)
        #
        # Delete the object
        yield session.delete(obj)
        # The backend id should not be in all model keys
        lkeys = yield backend.model_keys(self.model._meta)
        self.assertFalse(be.id in lkeys)
