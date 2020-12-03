"""Test router registration"""
from examples.models import SimpleModel

from stdnet import AlreadyRegistered, odm
from stdnet.utils import test


class TestRegistration(test.TestWrite):
    def register(self):
        router = odm.Router(self.backend)
        self.assertEqual(router.default_backend, self.backend)
        router.register_applications("examples")
        self.assertTrue(router)
        return router

    def test_registered_models(self):
        router = self.register()
        for meta in router.registered_models:
            name = meta.name
            self.assertEqual(meta.app_label, "examples")
            manager = router[meta]
            model = manager.model
            self.assertEqual(manager, getattr(router, name))
            self.assertEqual(manager, router[manager])
            self.assertEqual(manager, router[model])
            self.assertEqual(model._meta, manager._meta)
            self.assertFalse(meta.abstract)
            self.assertTrue(manager.backend)

    def test_unregister_all(self):
        router = self.register()
        self.assertTrue(router.registered_models)
        self.assertEqual(router.unregister(self), None)
        self.assertTrue(router.registered_models)
        N = len(router.registered_models)
        managers = router.unregister()
        self.assertEqual(N, len(managers))
        self.assertFalse(router.registered_models)

    def testFlushModel(self):
        router = self.register()
        yield router.flush()

    def test_flush_exclude(self):
        models = self.register()
        s = yield models.simplemodel.new(code="test")
        all = yield models.simplemodel.all()
        self.assertEqual(len(all), 1)
        yield models.flush(exclude=("examples.simplemodel",))
        all = yield models.simplemodel.all()
        self.assertEqual(len(all), 1)
        self.assertEqual(all[0], s)
        yield models.flush()
        all = yield models.simplemodel.all()
        self.assertFalse(all)

    def testFromUuid(self):
        models = self.register()
        s = yield models.simplemodel.new(code="test")
        uuid = s.uuid
        s2 = yield models.from_uuid(s.uuid)
        self.assertEqual(s, s2)
        yield self.async.assertRaises(
            odm.StdModel.DoesNotExist, models.from_uuid, "ccdscscds"
        )
        yield self.async.assertRaises(
            odm.StdModel.DoesNotExist, models.from_uuid, "ccdscscds.1"
        )
        a, b = tuple(uuid.split("."))
        yield self.async.assertRaises(
            odm.StdModel.DoesNotExist, models.from_uuid, "{0}.5".format(a)
        )

    def testFailedHashModel(self):
        self.assertRaises(KeyError, odm.hashmodel, SimpleModel)
