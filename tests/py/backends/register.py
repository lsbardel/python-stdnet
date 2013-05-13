from stdnet import odm, AlreadyRegistered
from stdnet.utils import test

from examples.models import SimpleModel


class TestRegistration(test.TestWrite):

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
        models = self.register()
        yield models.flush()

    def test_flush_exclude(self):
        models = self.register()
        s = yield models.simplemodel.new(code='test')
        models.flush(exclude=('examples.simplemodel',))
        all = yield models.simplemodel.all()
        self.assertEqual(len(all), 1)
        self.assertEqual(all[0], s)
        yield models.flush()
        all = yield models.simplemodel.all()
        self.assertFalse(all)

    def testFromUuid(self):
        models = self.register()
        s = yield models.simplemodel.new(code='test')
        uuid = s.uuid
        s2  = yield models.from_uuid(s.uuid)
        self.assertEqual(s, s2)
        yield self.async.assertRaises(SimpleModel.DoesNotExist,
                                      models.from_uuid, 'ccdscscds')
        yield self.async.assertRaises(SimpleModel.DoesNotExist,
                                      models.from_uuid, 'ccdscscds.1')
        a,b = tuple(uuid.split('.'))
        yield self.async.assertRaises(SimpleModel.DoesNotExist,
                                      models.from_uuid, '{0}.5'.format(a))

    def testFailedHashModel(self):
        self.assertRaises(KeyError, odm.hashmodel, SimpleModel)

