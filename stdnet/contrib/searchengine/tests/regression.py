from random import randint

from stdnet import test

from stdnet.utils import to_string, range
from stdnet.contrib.searchengine import SearchEngine, double_metaphone, stdnet_processor
from stdnet.contrib.searchengine.models import Word, WordItem, AutoComplete
from stdnet.utils import populate

from .basicwords import basic_english_words

from stdnet.contrib.searchengine.tests.testsearch.models import Item


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


NUM_WORDS = 40
WORDS_GROUPS = lambda size : (' '.join(populate('choice', NUM_WORDS,\
                              choice_from = basic_english_words)) for i in range(size))


def make_items(num = 100, content = False):
    names = populate('choice', num, choice_from=basic_english_words)
    if content:
        contents = WORDS_GROUPS(num)
    else:
        contents = ['']*num
    for name,co in zip(names,contents):
        if len(name) > 3:
            Item(name=name,
                 counter=randint(0,10),
                 content = co).save(commit = False)
    Item.commit()
    

class TestBase(object):
    metaphone = True
    autocomplete = None
    
    def register(self):
        self.engine = SearchEngine(metaphone = self.metaphone,
                                   autocomplete = self.autocomplete)
        self.orm.register(AutoComplete)
        self.orm.register(Word)
        self.orm.register(WordItem)
        self.orm.register(Item)
    
    def unregister(self):
        self.orm.unregister(AutoComplete)
        self.orm.unregister(Word)
        self.orm.unregister(WordItem)
        self.orm.unregister(Item)
    
    def simpleadd(self):
        engine = self.engine
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
    
    def sometags(self, num = 10, minlen = 3):
        def _():
            for tag in populate('choice',num,choice_from=basic_english_words):
                if len(tag) >= minlen:
                    yield tag
        return ' '.join(_())


class TestCase(TestBase,test.TestCase):
    
    def setUp(self):
        self.register()


class TestSearchEngine(TestCase):
    
    def testMetaphone(self):
        '''Test metaphone algorithm'''
        for name in NAMES:
            d = double_metaphone(name)
            self.assertEqual(d,NAMES[name])

    def testSimpleAdd(self):
        self.simpleadd()
        
    def testDoubleEntries(self):
        engine = self.engine
        item = self.simpleadd()
        wi = engine.index_item(item)
        self.assertFalse(wi)
        
    def testAddTag(self):
        item = self.make_item()
        engine = self.engine
        wi = engine.add_tag(item, 'language programming')
        self.assertTrue(wi)
        self.assertEqual(len(wi),2)
        tags = engine.tags_for_item(item)
        self.assertEqual(len(tags),2)
        tags = engine.alltags()
        self.assertEqual(len(tags),2)
        
    def testAddTags(self):
        engine = self.engine
        make_items(num=100)
        for item in Item.objects.all():
            self.assertTrue(engine.add_tag(item,self.sometags()))
        tags = self.engine.alltags()
        self.assertTrue(tags)
    
    
class TestSearchEngineWithRegistration(TestCase):
    
    def setUp(self):
        super(TestSearchEngineWithRegistration,self).setUp()
        self.engine.register(Item)
    
    def make_item(self,**kwargs):
        item = super(TestSearchEngineWithRegistration,self).make_item(**kwargs)
        wis = WordItem.objects.filter(model_type = item.__class__,
                                     object_id = item.id)
        self.assertTrue(wis)
        for wi in wis:
            self.assertEqual(wi.object,item)
        
        return item
        
    def testAdd(self):
        self.make_item()
        
    def testDelete(self):
        item = self.make_item()
        words = list(Word.objects.all())
        item.delete()
        wis = WordItem.objects.filter(model_type = item.__class__,
                                     object_id = item.id)
        self.assertFalse(wis)
        self.assertEqual(len(words),len(Word.objects.all()))
    
    
class TestAutoComplete(TestCase):
    autocomplete = 'en'
    
    def testMeta(self):
        auto = self.engine.autocomplete
        self.assertTrue(auto)
        self.assertEqual(auto.id,'en')
        self.assertEqual(len(auto.data),0)
        
    def testSimpleAdd(self):
        engine = self.engine
        item = self.make_item('fantastic',content='and')
        item2 = self.make_item('world',content='the')
        engine.index_item(item)
        engine.index_item(item)
        auto = engine.autocomplete
        self.assertEqual(len(auto.data),8)
        engine.index_item(item2)
        self.assertEqual(len(auto.data),12)
        items = [to_string(v) for v in auto.data]
        self.assertEqual(items,['fa','fan','fant','fanta',
                                'fantas','fantast','fantasti',
                                'fantastic*',
                                'wo','wor','worl','world*'])
        
    def testSearch(self):
        engine = self.engine
        make_items(content = True)
        for item in Item.objects.all():
            self.assertTrue(engine.index_item(item))
        search = engine.search('tro')
        self.assertTrue(search)
        
        