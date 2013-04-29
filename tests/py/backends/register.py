from stdnet import odm, AlreadyRegistered
from stdnet.utils import test

from examples.models import SimpleModel


class TestRegistration(test.TestCase):

    def register(self):
        router = odm.Router(self.backend)
        self.assertEqual(router.default_backend, self.backend)
        router.register_applications('examples')
        self.assertTrue(router)
        return router

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
        self.assertEqual(odm.unregister(self), 0)
        self.assertEqual(set(apps),set(odm.registered_models()))
        odm.unregister()
        for model in apps:
            manager = model.objects
            self.assertFalse(manager.backend)
            self.assertEqual(manager.model, model)
            self.assertRaises(ModelNotRegistered, manager.session)

    def testFlushModel(self):
        self.register()
        odm.flush_models()

    def testFlushExclude(self):
        self.register()
        SimpleModel(code = 'test').save()
        odm.flush_models(excludes = ('examples.simplemodel',))
        qs = SimpleModel.objects.query()
        self.assertTrue(qs)
        odm.flush_models()
        qs = SimpleModel.objects.query()
        self.assertFalse(qs)

    def testFromUuid(self):
        self.register()
        s = SimpleModel(code='test').save()
        uuid = s.uuid
        s2  = odm.from_uuid(s.uuid)
        self.assertEqual(s,s2)
        self.assertRaises(SimpleModel.DoesNotExist,odm.from_uuid, 'ccdscscds')
        self.assertRaises(SimpleModel.DoesNotExist,odm.from_uuid, 'ccdscscds.1')
        a,b = tuple(uuid.split('.'))
        self.assertRaises(SimpleModel.DoesNotExist,
                          odm.from_uuid,'{0}.5'.format(a))

    def testFailedHashModel(self):
        self.assertRaises(KeyError, odm.hashmodel, SimpleModel)

