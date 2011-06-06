from stdnet import test, orm

from examples.models import SimpleModel


class TestRegistration(test.TestCase):

    def setUp(self):
        self.apps = orm.register_applications('examples')
        self.assertTrue(self.apps)
        
    def unregister(self):
        for app in self.apps:
            orm.unregister(app)
        
    def testFlushModel(self):
        orm.flush_models()
        
    def testFlushExclude(self):
        SimpleModel(code = 'test').save()
        orm.flush_models(excludes = ('examples.simplemodel',))
        qs = SimpleModel.objects.all()
        self.assertTrue(qs)
        orm.flush_models()
        qs = SimpleModel.objects.all()
        self.assertFalse(qs)
        
    def testFromUuid(self):
        s = SimpleModel(code = 'test').save()
        uuid = s.uuid
        s2  = orm.from_uuid(s.uuid)
        self.assertEqual(s,s2)
        self.assertRaises(SimpleModel.DoesNotExist,orm.from_uuid,'cdcdscscds')
        self.assertRaises(SimpleModel.DoesNotExist,orm.from_uuid,'cdcdscscds.1')
        a,b = tuple(uuid.split('.'))
        self.assertRaises(SimpleModel.DoesNotExist,orm.from_uuid,'{0}.5'.format(a))