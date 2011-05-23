from stdnet import test
from stdnet.utils import zip

from .regression import make_items, TestBase, Item


class TestIndexItem(TestBase,test.ProfileTest):
    
    def initialise(self):
        make_items(200,content = True)
        
    def run(self):
        engine = self.engine
        for item in Item.objects.all():
            engine.index_item(item)
    