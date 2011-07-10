from stdnet import test
from stdnet.utils import populate, zip

from profile.delete import tests as dtest


class PipeDeleteTest(dtest.DeleteTest):                        
    
    def run(self):
        self.model.objects.all().delete()

