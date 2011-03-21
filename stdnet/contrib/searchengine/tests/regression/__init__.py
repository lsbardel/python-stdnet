from stdnet import test

from stdnet.utils import to_string
from stdnet.contrib.searchengine.models import AutoComplete, autocomplete
#from examples.spelling.spelling import NWORDS


class TestAutoComplete(test.TestCase):
    
    def setUp(self):
        self.orm.register(AutoComplete)
        
    def unregister(self):
        self.orm.unregister(AutoComplete)
         
    def testAdd(self):
        auto = autocomplete()
        auto.add('fantastic')
        auto.add('fantastic')
        self.assertEqual(len(auto.data),8)
        auto.add('world')
        self.assertEqual(len(auto.data),12)
        items = [to_string(v) for v in auto.data]
        self.assertEqual(items,['fa','fan','fant','fanta',
                                'fantas','fantast','fantasti',
                                'fantastic*',
                                'wo','wor','worl','world*'])