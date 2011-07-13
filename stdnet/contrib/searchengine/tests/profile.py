from stdnet import test
from stdnet.utils import zip

from .fuzzy import makeItems, Item


class TestIndexItem(test.ProfileTest):
    
    def initialise(self):
        makeItems(100,300)
        
    def run(self):
        engine = self.engine.index_item
        for item in Item.objects.all():
            engine(item)
    