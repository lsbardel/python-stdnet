from stdnet import test, orm
from stdnet.utils import zip

from .fuzzy import makeItems, Item, Word, WordItem, SearchEngine


class TestIndexItem(test.ProfileTest):
    
    def register(self):
        self.engine = SearchEngine(autocomplete = False)
        orm.register(Word)
        orm.register(Item)
        orm.register(WordItem)
        
    def setUp(self):
        makeItems(100,300)
        
    def run(self):
        engine = self.engine.index_item
        for item in Item.objects.all():
            engine(item)
    