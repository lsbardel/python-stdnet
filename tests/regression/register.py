from stdnet import test, orm, ModelNotRegistered

from examples.models import SimpleModel


class TestRegistration(test.TestCase):

    def _register(self):
        apps = orm.register_applications('examples')
        self.assertTrue(apps)
        return apps
        
    def testRegisterApplications(self):
        for model in self._register():
            manager = model.objects
            meta = model._meta
            self.assertFalse(meta.abstract)
            self.assertTrue(manager.session)
            self.assertEqual(manager.model,model)
            self.assertEqual(manager.backend,manager.session().backend)
            self.assertEqual(meta.app_label,'examples')
        
    def testUnregisterAll(self):
        apps = self._register()
        orm.unregister()
        for model in apps:
            manager = model.objects
            self.assertFalse(manager.backend)
            self.assertEqual(manager.model,model)
            self.assertRaises(ModelNotRegistered, manager.session)
        
    def testFlushModel(self):
        self._register()
        orm.flush_models()
        
    def testFlushExclude(self):
        self._register()
        SimpleModel(code = 'test').save()
        orm.flush_models(excludes = ('examples.simplemodel',))
        qs = SimpleModel.objects.all()
        self.assertTrue(qs)
        orm.flush_models()
        qs = SimpleModel.objects.all()
        self.assertFalse(qs)
        
    def testFromUuid(self):
        self._register()
        s = SimpleModel(code = 'test').save()
        uuid = s.uuid
        s2  = orm.from_uuid(s.uuid)
        self.assertEqual(s,s2)
        self.assertRaises(SimpleModel.DoesNotExist,orm.from_uuid,'cdcdscscds')
        self.assertRaises(SimpleModel.DoesNotExist,orm.from_uuid,'cdcdscscds.1')
        a,b = tuple(uuid.split('.'))
        self.assertRaises(SimpleModel.DoesNotExist,orm.from_uuid,'{0}.5'.format(a))
        
