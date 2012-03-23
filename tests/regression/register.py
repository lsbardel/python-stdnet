from stdnet import test, orm, ModelNotRegistered, AlreadyRegistered

from examples.models import SimpleModel


class TestRegistration(test.TestCase):

    def register(self):
        apps = orm.register_applications('examples', default = self.backend)
        self.assertTrue(apps)
        return apps
        
    def testRegisterApplications(self):
        for model in self.register():
            manager = model.objects
            meta = model._meta
            self.assertFalse(meta.abstract)
            self.assertTrue(manager.session)
            self.assertEqual(manager.model,model)
            self.assertEqual(manager.backend,manager.session().backend)
            self.assertEqual(meta.app_label,'examples')
        
    def testUnregisterAll(self):
        apps = self.register()
        self.assertEqual(orm.unregister(self), None)
        self.assertEqual(set(apps),set(orm.registered_models()))
        orm.unregister()
        for model in apps:
            manager = model.objects
            self.assertFalse(manager.backend)
            self.assertEqual(manager.model,model)
            self.assertRaises(ModelNotRegistered, manager.session)
        
    def testFlushModel(self):
        self.register()
        orm.flush_models()
        
    def testFlushExclude(self):
        self.register()
        SimpleModel(code = 'test').save()
        orm.flush_models(excludes = ('examples.simplemodel',))
        qs = SimpleModel.objects.query()
        self.assertTrue(qs)
        orm.flush_models()
        qs = SimpleModel.objects.query()
        self.assertFalse(qs)
        
    def testFromUuid(self):
        self.register()
        s = SimpleModel(code = 'test').save()
        uuid = s.uuid
        s2  = orm.from_uuid(s.uuid)
        self.assertEqual(s,s2)
        self.assertRaises(SimpleModel.DoesNotExist,orm.from_uuid,'cdcdscscds')
        self.assertRaises(SimpleModel.DoesNotExist,orm.from_uuid,'cdcdscscds.1')
        a,b = tuple(uuid.split('.'))
        self.assertRaises(SimpleModel.DoesNotExist,
                          orm.from_uuid,'{0}.5'.format(a))
        
    def testFailedHashModel(self):
        self.assertRaises(KeyError, orm.hashmodel, SimpleModel)
        
    def testAlreadyRegistered(self):
        self.register()
        self.assertEqual(orm.register(SimpleModel),None)
        self.assertRaises(AlreadyRegistered, orm.register, SimpleModel,
                          ignore_duplicates = False)
        