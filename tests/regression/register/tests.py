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