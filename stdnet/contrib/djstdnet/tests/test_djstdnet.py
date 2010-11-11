
from django.core.management import setup_environ
import settings
setup_environ(settings)
from django.conf import settings

from stdnet import orm, test
from stdnet.contrib.djstdnet.djlink import link_models
from testmodel.models import DataId, Data


class DjStdNetTest(test.TestCase):
    
    def setUp(self):
        orm.register(Data)
        link_models(DataId,Data)
        
    def unregister(self):
        orm.unregister(Data)
    
    def testLinks(self):
        self.assertEqual(Data._meta.linked,DataId)
        self.assertEqual(DataId._meta.linked,Data)
    
    def tearDown(self):
        orm.clearall()
        self.unregister()
