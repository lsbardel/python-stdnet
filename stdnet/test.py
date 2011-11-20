'''\
A specialized test case class for stdnet
'''
import os
import sys
import logging
from inspect import isclass

import sys

if sys.version_info >= (2,7):
    import unittest
else:
    try:
        import unittest2 as unittest
    except ImportError:
        print('To run tests in python 2.6 you need to install\
 the unitest2 package')
        exit(0)

from stdnet import orm
from stdnet.utils import to_string, gen_unique_id


class TestCase(unittest.TestCase):
    '''Base class for testing stdnet.'''    
    models = ()
    model = None
    
    def register(self):
        if not self.models and self.model:
            self.models = (self.model,)
        for model in self.models:
            orm.register(model)
        
    def unregister(self):
        orm.unregister()
    
    def _pre_setup(self):
        self.prefix = 'test-'+gen_unique_id()+'.'
        self.register()
        for model in orm.registered_models():
            model._meta.keyprefix = self.prefix
        
    def _post_teardown(self):
        for model in orm.registered_models():
            model.flush()
        #orm.clearall()
        self.unregister()
    
    def __call__(self, result=None):
        """Wrapper around default __call__ method
to perform cleanup, registration and unregistration.
        """
        self._pre_setup()
        super(TestCase, self).__call__(result)
        self._post_teardown()
        
    def cleankeys(self, meta):
        tmp = meta.basekey('tmp')
        keys = []
        for key in meta.cursor.keys():
            if not key.startswith(tmp):
                keys.append(key)
        return keys
                    
        
class TestMultiFieldMixin(object):
    '''Test class which add a couple of tests for multi fields. You need to implement the
    get_object_and_field and adddata methods'''
    
    def get_object_and_field(self):
        raise NotImplementedError
    
    def adddata(self, obj):
        raise NotImplementedError
    
    def testMultiFieldId(self):
        '''Here we check for multifield specific stuff like the instance
related keys (keys which are related to the instance rather than the model).'''
        # get instance and field, the field has no data here
        obj, field = self.get_object_and_field()
        # get the object id
        id = to_string(obj.id)
        # get the field database key
        field_key = to_string(field.id)
        self.assertTrue(id in field_key)
        keys = obj.instance_keys()
        # field id should be in instance keys
        self.assertTrue(field.id in keys)
        lkeys = list(obj._meta.cursor.keys())
        # the field has no data, so there is no key in the database
        self.assertFalse(field.id in lkeys)
        #
        # Lets add data
        self.adddata(obj)
        # The field id should be in the server keys
        lkeys = obj._meta.cursor.keys()
        self.assertTrue(field.id in lkeys)
        obj.delete()
        lkeys = list(obj._meta.cursor.keys())
        self.assertFalse(field.id in lkeys)
        

 