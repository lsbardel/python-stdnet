__benchmark__ = True
from stdnet import test, orm
from stdnet.utils import zip

from . import regression
from .fuzzy import makeItems, Item, Word, WordItem, SearchEngine


class TestIndexItem(regression.TestCase):
    autocomplete = False
        
    def setUp(self):
        makeItems(100,300)
        
    def run(self):
        engine = self.engine.index_item
        for item in Item.objects.all():
            engine(item)
    