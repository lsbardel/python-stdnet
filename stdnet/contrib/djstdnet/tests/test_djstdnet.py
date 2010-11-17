
from django.core.management import setup_environ
import settings
setup_environ(settings)
from django.conf import settings

from stdnet import orm, test
from stdnet.contrib.djstdnet.djlink import link_models, LinkedManager
from testmodel.models import DataId, Data, Environment



class DjStdNetTest(test.TestCase):
    tags = ['django','monitor']
    
    def setUp(self):
        orm.register(Data)
        orm.register(Environment)
        link_models(DataId,Data)
        
    def unregister(self):
        orm.unregister(Data)
    
    def testLinked(self):
        self.assertEqual(Data._meta.linked,DataId)
        self.assertEqual(DataId._meta.linked,Data)
        self.assertTrue(isinstance(Data.objects,LinkedManager))
        
    def testDerivedManager(self):
        self.assertFalse(isinstance(Environment.objects,LinkedManager))
    
    def tearDown(self):
        orm.clearall()
        self.unregister()