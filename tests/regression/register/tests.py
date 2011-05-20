from stdnet import test, orm


class TestRegistration(test.TestCase):

    def setUp(self):
        self.apps = orm.register_applications('examples')
        self.assertTrue(self.apps)
        
    def unregister(self):
        for app in self.apps:
            orm.unregister(app)
        
    def testFlushModel(self):
        orm.flush_models()