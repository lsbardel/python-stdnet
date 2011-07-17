from random import randint

from stdnet import test

from stdnet.utils import to_string, range
from stdnet.contrib.searchengine import SearchEngine, double_metaphone
from stdnet.contrib.searchengine.models import Word, WordItem, AutoComplete
from stdnet.utils import populate

from .basicwords import basic_english_words

from .testsearch.models import Item, RelatedItem


python_content = 'Python is a programming language that lets you work more\
 quickly and integrate your systems more effectively.\
 You can learn to use Python and see almost immediate gains\
 in productivity and lower maintenance costs.'


NAMES = {'maurice':('MRS', None),
         'aubrey':('APR', None),
         'cambrillo':('KMPRL','KMPR'),
         'heidi':('HT', None),
         'katherine':('K0RN','KTRN'),
         'Thumbail':('0MPL','TMPL'),
         'catherine':('K0RN','KTRN'),
         'richard':('RXRT','RKRT'),
         'bob':('PP', None),
         'eric':('ARK', None),
         'geoff':('JF','KF'),
         'Through':('0R','TR'),
         'Schwein':('XN', 'XFN'),
         'dave':('TF', None),
         'ray':('R', None),
         'steven':('STFN', None),
         'bryce':('PRS', None),
         'randy':('RNT', None),
         'bryan':('PRN', None),
         'Rapelje':('RPL', None),
         'brian':('PRN', None),
         'otto':('AT', None),
         'auto':('AT', None),
         'Dallas':('TLS', None),
         'maisey':('MS', None),
         'zhang':('JNK', None),
         'Chile':('XL', None),
         'Jose':('HS', None),
         'Arnow':('ARN','ARNF'),
         'solilijs':('SLLS', None),
         'Parachute':('PRKT', None),
         'Nowhere':('NR', None),
         'Tux':('TKS', None)}


NUM_WORDS = 40
WORDS_GROUPS = lambda size : (' '.join(populate('choice', NUM_WORDS,\
                              choice_from = basic_english_words))\
                               for i in range(size))


def make_items(num = 30, content = False, related = None):
    names = populate('choice', num, choice_from=basic_english_words)
    if content:
        contents = WORDS_GROUPS(num)
    else:
        contents = ['']*num
    with Item.transaction() as t:
        for name,co in zip(names,contents):
            if len(name) > 3:
                Item(name=name,
                     counter=randint(0,10),
                     content = co,
                     related = related).save(t)


class TestCase(test.TestCase):
    '''Mixin for testing the search engine. No tests implemented here,
just registration and some utility functions. All searchengine tests
below will derive from this class.'''
    metaphone = True
    autocomplete = None
        
    def register(self):
        self.engine = SearchEngine(metaphone = self.metaphone,
                                   autocomplete = self.autocomplete)
        self.orm.register(AutoComplete)
        self.orm.register(Word)
        self.orm.register(WordItem)
        self.orm.register(Item)
        self.orm.register(RelatedItem)
        self.engine.register(Item,('related',))
    
    def unregister(self):
        self.orm.unregister(AutoComplete)
        self.orm.unregister(Word)
        self.orm.unregister(WordItem)
        self.orm.unregister(Item)
        self.orm.unregister(RelatedItem)
    
    def simpleadd(self,name='python',counter=10,content=None,related=None):
        engine = self.engine
        item = self.make_item(name,counter,content,related)
        wi = WordItem.objects.filter(model_type = Item, object_id = item.id)
        self.assertTrue(wi)
        return item,wi
    
    def make_item(self,name='python',counter=10,content=None,related=None):
        return Item(name=name,
                    counter = counter,
                    content=content or python_content,
                    related= related).save()
    
    def sometags(self, num = 10, minlen = 3):
        def _():
            for tag in populate('choice',num,choice_from=basic_english_words):
                if len(tag) >= minlen:
                    yield tag
        return ' '.join(_())



class TestMeta(TestCase):
    '''Test internal functions, not the API.'''
    def testSplitting(self):
        eg = SearchEngine(metaphone = False, stemming = False)
        self.assertEqual(list(eg.words_from_text('bla-ciao+pippo')),\
                         ['bla','ciao','pippo'])
        self.assertEqual(list(eg.words_from_text('bla.-ciao:;pippo')),\
                         ['bla','ciao','pippo'])
        self.assertEqual(list(eg.words_from_text('  bla ; @ciao ;:`')),\
                         ['bla','ciao'])
        self.assertEqual(list(eg.words_from_text('bla bla____bla')),\
                         ['bla','bla','bla'])
        
    def testMetaphone(self):
        '''Test metaphone algorithm'''
        for name in NAMES:
            d = double_metaphone(name)
            self.assertEqual(d,NAMES[name])
    
    def testRegistered(self):
        self.assertTrue(Item in self.engine.REGISTERED_MODELS)
                
    def testWordModel(self):
        # This tests was put in place bacuse the Word model was
        # not working properly in Python 3
        Word(id = 'bla').save()
        w = Word.objects.get(id = 'bla')
        self.assertFalse(isinstance(w.id,bytes))
        
        
class TestSearchEngine(TestCase):

    def testSimpleAdd(self):
        self.simpleadd()
        
    def testDoubleEntries(self):
        '''Test an item indexed twice'''
        engine = self.engine
        item,wi = self.simpleadd()
        wi = set((w.word for w in wi))
        wi2 = engine.index_item(item)
        wi2 = set((w.word for w in wi2))
        self.assertEqual(wi,wi2)
        
    def testSearchWords(self):
        self.simpleadd()
        words = self.engine.words('python gains')
        self.assertTrue(len(words)>=2)
        
    def testSearchModelSimple(self):
        item,_ = self.simpleadd()
        qs = Item.objects.search('python gains')
        self.assertEqual(len(qs.queries),3)
        self.assertEqual(qs.queries[0].field,'object_id')
        self.assertEqual(qs.queries[1].field,'object_id')
        self.assertEqual(qs.queries[2].field,'object_id')
        self.assertEqual(qs.count(),1)
        self.assertEqual(item,qs[0])
        
    def testSearchModel(self):
        item1,wi1 = self.simpleadd()
        item2,wi2 = self.simpleadd('pink',content='the dark side of the moon')
        item3,wi3 = self.simpleadd('queen',content='we will rock you')
        item4,wi4 = self.simpleadd('python',content='nothing here')
        qs = Item.objects.search('python')
        self.assertEqual(qs.count(),2)
        qs = Item.objects.search('python learn')
        for q in qs.queries[:]:
            wis = q.query[:]
            self.assertTrue(wis)
        self.assertEqual(qs.count(),1)
        self.assertEqual(qs[0].name,'python')
        
    def testRelatedModel(self):
        r = RelatedItem(name = 'planet earth is wonderful').save()
        self.simpleadd('king',content='england')
        self.simpleadd('nothing',content='empty', related = r)
        qs = Item.objects.search('planet')
        self.assertEqual(qs.count(),1)
        
    def _testAddTag(self):
        item = self.make_item()
        engine = self.engine
        wi = engine.add_tag(item, 'language programming')
        self.assertTrue(wi)
        self.assertEqual(len(wi),2)
        tags = engine.tags_for_item(item)
        self.assertEqual(len(tags),2)
        tags = engine.alltags()
        self.assertEqual(len(tags),2)
        
    def _testAddTags(self):
        engine = self.engine
        make_items(num=100)
        for item in Item.objects.all():
            self.assertTrue(engine.add_tag(item,self.sometags()))
        tags = self.engine.alltags()
        self.assertTrue(tags)
    
    
class TestSearchEngineWithRegistration(TestCase):
        
    def make_item(self,**kwargs):
        item = super(TestSearchEngineWithRegistration,self).make_item(**kwargs)
        wis = WordItem.objects.filter(model_type = item.__class__)
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
        wis = WordItem.objects.filter(model_type = item.__class__)
        self.assertFalse(wis)
        self.assertEqual(len(words),len(Word.objects.all()))
    
    
class TestAutoComplete(object):
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
        
        