from random import randint

from stdnet import test

from stdnet.utils import to_string
from stdnet.contrib.searchengine import engine, double_metaphone
from stdnet.contrib.searchengine.models import Word, WordItem
from stdnet.utils import populate

from .basicwords import basic_english_words

from .testsearch.models import Item

python_content = 'Python is a programming language that lets you work more\
 quickly and integrate your systems more effectively.\
 You can learn to use Python and see almost immediate gains\
 in productivity and lower maintenance costs.'


NAMES = {'maurice':('MRS', None),'aubrey':('APR', None),'cambrillo':('KMPRL','KMPR')\
        ,'heidi':('HT', None),'katherine':('K0RN','KTRN'),'Thumbail':('0MPL','TMPL')\
        ,'catherine':('K0RN','KTRN'),'richard':('RXRT','RKRT'),'bob':('PP', None)\
        ,'eric':('ARK', None),'geoff':('JF','KF'),'Through':('0R','TR'), 'Schwein':('XN', 'XFN')\
        ,'dave':('TF', None),'ray':('R', None),'steven':('STFN', None),'bryce':('PRS', None)\
        ,'randy':('RNT', None),'bryan':('PRN', None),'Rapelje':('RPL', None)\
        ,'brian':('PRN', None),'otto':('AT', None),'auto':('AT', None), 'Dallas':('TLS', None)\
        , 'maisey':('MS', None), 'zhang':('JNK', None), 'Chile':('XL', None)\
        ,'Jose':('HS', None), 'Arnow':('ARN','ARNF'), 'solilijs':('SLLS', None)\
        , 'Parachute':('PRKT', None), 'Nowhere':('NR', None), 'Tux':('TKS', None)}

class TestSearchEngine(test.TestCase):

    def setUp(self):
        self.orm.register(Word)
        self.orm.register(WordItem)
        self.orm.register(Item)
        
    def unregister(self):
        self.orm.unregister(Word)
        self.orm.unregister(WordItem)
        self.orm.unregister(Item)
    
    def testMetaphone(self):
        for name in NAMES:
            d = double_metaphone(name)
            self.assertEqual(d,NAMES[name])

    def testSimpleAdd(self):
        self.simpleadd()
        
    def testDoubleEntries(self):
        item = self.simpleadd()
        wi = engine.index_item(item)
        self.assertFalse(wi)
        
    def testAddTag(self):
        item = self.make_item()
        wi = engine.add_tag(item, 'language programming')
        self.assertTrue(wi)
        self.assertEqual(len(wi),2)
        tags = engine.tags_for_item(item)
        self.assertEqual(len(tags),2)
        tags = engine.alltags()
        self.assertEqual(len(tags),2)
        
    def testAddTags(self):
        self.make_items(num=100)
        for item in Item.objects.all():
            self.assertTrue(engine.add_tag(item,self.sometags()))
        tags = engine.alltags()
        self.assertTrue(tags)
    
    # Utilities
    
    def simpleadd(self):
        item = self.make_item()
        wi = engine.index_item(item)
        self.assertTrue(wi)
        result = list(engine.search('python'))
        self.assertEqual(len(result),1)
        self.assertEqual(result[0],item)
        result = list(engine.search('python learn'))
        self.assertEqual(len(result),1)
        self.assertEqual(result[0],item)
        return item
    
    def make_item(self,name='python',counter=10,content=None):
        return Item(name=name,
                    counter = counter,
                    content=content or python_content).save()
                    
    def make_items(self, num = 200):
        for name in populate('choice', num, choice_from=basic_english_words):
            if len(name) > 3:
                Item(name=name,
                     counter=randint(0,10)).save(commit = False)
        Item.commit()
    
    def sometags(self, num = 10, minlen = 3):
        def _():
            for tag in populate('choice',num,choice_from=basic_english_words):
                if len(tag) >= minlen:
                    yield tag
        return ' '.join(_())
                
    
class TestAutoComplete(object):
    
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