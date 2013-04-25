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
    
    @classmethod
    def after_setup(cls):
        cls.data = cls.data_cls(cls.size)
        
    def setUp(self):
        self.names = test.populate('string', size=10)
        self.name = self.names[0]
        
    def defaults(self):
        return {}
        
    def get_object_and_field(self, save=True, **kwargs):
        params = self.defaults()
        params.update(kwargs)
        m = self.model(**params)
        if save:
            with self.session().begin() as t:
                t.add(m)
            yield t.on_result
        yield m, getattr(m, self.attrname)
    
    def adddata(self, obj):
        raise NotImplementedError
    
    def test_RaiseStructFieldError(self):
        yield self.async.assertRaises(StructureFieldError,
                                      self.get_object_and_field, False)
    
    def test_multiFieldId(self):
        '''Here we check for multifield specific stuff like the instance
related keys (keys which are related to the instance rather than the model).'''
        # get instance and field, the field has no data here
        obj, field = yield self.get_object_and_field()
        # get the object id
        id = to_string(obj.id)
        # get the field database key
        field_key = to_string(field.id)
        self.assertTrue(id in field_key)
        #
        backend = obj.session.backend
        keys = backend.instance_keys(obj)
        if backend.name == 'redis':
            # field id should be in instance keys
            self.assertTrue(field.id in keys)
            lkeys = yield backend.model_keys(self.model._meta)
            # the field has no data, so there is no key in the database
            self.assertFalse(field.id in lkeys)
        #
        # Lets add data
        self.adddata(obj)
        # The field id should be in the server keys
        if backend.name == 'redis':
            lkeys = list(backend.model_keys(self.model._meta))
            self.assertTrue(field.id in lkeys)
        obj.delete()
        lkeys = list(backend.model_keys(self.model._meta))
        self.assertFalse(field.id in lkeys)
