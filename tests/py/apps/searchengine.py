'''Search engine application in `apps.searchengine`.'''
from random import randint
from datetime import date

from stdnet import odm, QuerySetError
from stdnet.utils import test, to_string, range, populate
from stdnet.odm.search import UpdateSE
from stdnet.apps.searchengine import SearchEngine, processors
from stdnet.apps.searchengine.models import WordItem

from examples.wordsearch.basicwords import basic_english_words
from examples.wordsearch.models import Item, RelatedItem
from examples.models import SimpleModel


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
    
    
class TestCase(test.CleanTestCase):
    '''Mixin for testing the search engine. No tests implemented here,
just registration and some utility functions. All search-engine tests
below will derive from this class.'''
    multipledb = 'redis'
    metaphone = True
    stemming = True
    models = (WordItem, Item, RelatedItem)
    
    def setUp(self):
        self.register()
        self.engine = self.make_engine()
        self.engine.register(Item,('related',))
        self.engine.register(RelatedItem,('related',))
        self.load_scripts()
    
    def make_engine(self):
        return SearchEngine(metaphone=self.metaphone, stemming=self.stemming)
        
    def make_item(self,name='python',counter=10,content=None,related=None):
        session = self.session()
        content = content if content is not None else python_content
        with session.begin():
            item = session.add(Item(name=name,
                                    counter = counter,
                                    content=content,
                                    related = related))
        return item
    
    def make_items(self, num=30, content=False, related=None):
        '''Bulk creation of Item for testing search engine. Return a set
of words which have been included in the Items.'''
        names = populate('choice', num, choice_from=basic_english_words)
        session = self.session()
        words = set()
        if content:
            contents = WORDS_GROUPS(num)
        else:
            contents = ['']*num
        with session.begin():
            for name, content in zip(names,contents):
                if len(name) > 3:
                    words.add(name)
                    if content:
                        words.update(content.split())
                    session.add(Item(name=name,
                                     counter=randint(0,10),
                                     content=content,
                                     related=related))
        wis = WordItem.objects.for_model(Item)
        self.assertTrue(wis.count())
        return words
    
    def simpleadd(self, name='python', counter=10, content=None, related=None):
        item = self.make_item(name, counter, content, related)
        wis = WordItem.objects.for_model(item).all()
        self.assertTrue(wis)
        for wi in wis:
            self.assertEqual(wi.object, item)
        return item, wis
    
    def sometags(self, num=10, minlen=3):
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
        
    def testSplitters(self):
        eg = SearchEngine(splitters = False)
        self.assertEqual(eg.punctuation_regex, None)
        words = list(eg.split_text('pippo:pluto'))
        self.assertEqual(len(words),1)
        self.assertEqual(words[0],'pippo:pluto')
        words = list(eg.split_text('pippo: pluto'))
        self.assertEqual(len(words),2)
        self.assertEqual(words[0],'pippo:')
        
    def testMetaphone(self):
        '''Test metaphone algorithm'''
        for name in NAMES:
            d = processors.double_metaphone(name)
            self.assertEqual(d,NAMES[name])
    
    def testRegistered(self):
        self.assertTrue(Item in self.engine.REGISTERED_MODELS)
        self.assertFalse(SimpleModel in self.engine.REGISTERED_MODELS)
        
    def testNoSearchEngine(self):
        session = self.session()
        query = session.query(SimpleModel)
        self.assertRaises(QuerySetError, query.search, 'bla')
        
    def testAddWithNumbers(self):
        item, wi = self.simpleadd(name = '20y', content = '')
        wi = list(wi)
        self.assertEqual(len(wi),1)
        wi = wi[0]
        self.assertEqual(str(wi.word),'20y')
        
        
class TestSearchEngine(TestCase):
    
    def testSimpleAdd(self):
        self.simpleadd()
        
    def testDoubleEntries(self):
        '''Test an item indexed twice'''
        engine = self.engine
        item, wi = self.simpleadd()
        wi = set((w.word for w in wi))
        item.save()
        wi2 = set((w.word for w in WordItem.objects.for_model(item)))
        # Lets get the words for item
        self.assertEqual(wi,wi2)
        
    def testSearchWords(self):
        self.simpleadd()
        words = list(self.engine.words_from_text('python gains'))
        self.assertTrue(len(words)>=2)
        
    def testSearchModelSimple(self):
        item,_ = self.simpleadd()
        qs = Item.objects.search('python gains')
        self.assertEqual(qs.text,('python gains',None))
        q = qs.construct()
        self.assertEqual(q.keyword,'intersect')
        self.assertEqual(len(q),4)
        self.assertEqual(qs.count(),1)
        self.assertEqual(item,qs[0])
        
    def testSearchModel(self):
        item1, wi1 = self.simpleadd()
        item2, wi2 = self.simpleadd('pink',content='the dark side of the moon')
        item3, wi3 = self.simpleadd('queen',content='we will rock you')
        item4, wi4 = self.simpleadd('python',content='nothing here')
        qs = Item.objects.search('python')
        qc = qs.construct()
        self.assertEqual(len(qc),3)
        self.assertEqual(qc.keyword,'intersect')
        self.assertEqual(qs.count(),2)
        qs = Item.objects.search('python learn')
        self.assertEqual(qs.count(),1)
        self.assertEqual(qs[0].name,'python')
        
    def testRelatedModel(self):
        r = RelatedItem(name = 'planet earth is wonderful').save()
        self.simpleadd('king',content='england')
        self.simpleadd('nothing',content='empty', related = r)
        qs = Item.objects.search('planet')
        qc = qs.construct()
        self.assertEqual(len(qc),2)
        self.assertEqual(qc.keyword,'intersect')
        self.assertEqual(qs.count(),1)
        
    def testBigSearch(self):
        words = self.make_items(num = 30, content = True)
        sw = ' '.join(populate('choice', 1, choice_from = words))
        qs = Item.objects.query().search(sw)
        self.assertTrue(qs)
        items = qs.all()
        
    def testInSearch(self):
        words = self.make_items(num = 30, content = True)
        sw = ' '.join(populate('choice', 5, choice_from = words))
        res1 = Item.objects.search(sw)
        res2 = Item.objects.search(sw, lookup = 'in')
        self.assertTrue(res2)
        self.assertTrue(res1.count() < res2.count())
        
    def testEmptySearch(self):
        words = self.make_items(num = 30, content = True)
        qs = set(self.engine.search('')[0].all())
        qs2 = set(WordItem.objects.query().all())
        self.assertTrue(qs)
        self.assertEqual(qs,qs2)
        self.assertRaises(ValueError, self.engine.search,
                          'first second ', lookup = 'foo')
        
    def testFlush(self):
        self.make_items()
        self.engine.flush()
        self.assertFalse(WordItem.objects.query())
        
    def testDelete(self):
        item = self.make_item()
        words = list(WordItem.objects.query())
        item.delete()
        wis = WordItem.objects.filter(model_type = item.__class__)
        self.assertFalse(wis.count(),0)
        
    def testSearch(self):
        words = self.make_items(num = 30, content = True)
        text = ' '.join(populate('choice', 1, choice_from = words))
        result = self.engine.search(text)
        self.assertTrue(result)
        
    def testNoWords(self):
        self.make_items(num = 30, content=True)
        q = set(Item.objects.search(''))
        all = set(Item.objects.query())
        self.assertEqual(q,all)
        
    def testReindex(self):
        self.make_items(num = 30, content=True)
        wis1 = set(WordItem.objects.query())
        self.engine.reindex()
        wis2 = set(WordItem.objects.query())
        self.assertEqual(wis1,wis2)
        
    def test_skip_indexing_when_missing_fields(self):
        session = self.session()
        item, wis = self.simpleadd()
        obj = session.query(Item).load_only('id').get(id=item.id)
        obj.save()
        wis2 = session.query(WordItem).filter(model_type=Item,
                                              object_id=obj.id).all()
        self.assertEqual(wis, wis2)
        

class TestCoverage(TestCase):
    
    def make_engine(self):
        eg = SearchEngine(metaphone = False)
        eg.add_word_middleware(processors.metaphone_processor)
        return eg
        
    def testAdd(self):
        item, wi = self.simpleadd('pink',
                                  content='the dark side of the moon 10y')
        wi = set((str(w.word) for w in wi))
        self.assertEqual(len(wi),4)
        self.assertFalse('10y' in wi)
        
    def testRepr(self):
        item, wi = self.simpleadd('pink',
                                  content='the dark side of the moon 10y')
        for w in wi:
            self.assertEqual(str(w),str(w.word))
            

class TestCoverageBaseClass(TestCase):
    
    def testAbstracts(self):
        e = odm.SearchEngine()
        self.assertRaises(NotImplementedError, e.search, 'bla')
        self.assertRaises(NotImplementedError, e.search_model, None, 'bla')
        self.assertRaises(NotImplementedError, e.flush)
        self.assertRaises(NotImplementedError, e.add_item, None, None, None)
        self.assertRaises(NotImplementedError, e.remove_item, None, None, None)
        self.assertEqual(e.session(), None)
        self.assertEqual(e.split_text('ciao luca'), ['ciao','luca'])
    
    def testItemFieldIterator(self):
        e = odm.SearchEngine()
        self.assertRaises(ValueError, e.item_field_iterator, None)
        u = UpdateSE(e)
        self.assertEqual(u.se, e)
        self.assertRaises(ValueError, u, None)