__benchmark__ = True

from tests.regression import search


class TestIndexItem(search.TestCase):
        
    def setUp(self):
        makeItems(100,300)
        
    def run(self):
        engine = self.engine.index_item
        for item in Item.objects.all():
            engine(item)
    