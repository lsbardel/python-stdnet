'''Search engine application in `apps.searchengine`.'''
from random import randint
from datetime import date

from stdnet import test
from stdnet.utils import to_string, range
from stdnet.apps.searchengine import SearchEngine, double_metaphone
from stdnet.apps.searchengine.models import Word, WordItem
from stdnet.utils import populate

from examples.wordsearch.basicwords import basic_english_words
from examples.wordsearch.models import Item, RelatedItem


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


class TestCase(test.TestCase):
    '''Mixin for testing the search engine. No tests implemented here,
just registration and some utility functions. All search-engine tests
below will derive from this class.'''
    metaphone = True
    stemming = True
    models = (Word, WordItem, Item, RelatedItem)
    
    def setUp(self):
        self.register()
        self.engine = SearchEngine(metaphone = self.metaphone,
                                   stemming = self.stemming)
        self.engine.register(Item,('related',))
    
    def make_item(self,name='python',counter=10,content=None,related=None):
        session = self.session()
        with session.begin():
            item = session.add(Item(name=name, counter = counter,
                    content=content if content is not None else python_content,
                    related = related))
        return item
    
    def make_items(self, num = 30, content = False, related = None):
        names = populate('choice', num, choice_from=basic_english_words)
        session = self.session()
        if content:
            contents = WORDS_GROUPS(num)
        else:
            contents = ['']*num
        with session.begin():
            for name,co in zip(names,contents):
                if len(name) > 3:
                    session.add(Item(name=name,
                                     counter=randint(0,10),
                                     content = co,
                                     related = related))
    
    def simpleadd(self, name = 'python', counter = 10, content = None,
                  related = None):
        item = self.make_item(name,counter,content,related)
        self.assertEqual(item.last_indexed.date(),date.today())
        wi = WordItem.objects.for_model(item)
        self.assertTrue(wi.count())
        return item, wi
    
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
        # This tests was put in place because the Word model was
        # not working properly in Python 3
        session = self.session()
        with session.begin():
            session.add(Word(id = 'bla'))
        w = session.query(Word).get(id = 'bla')
        self.assertFalse(isinstance(w.id,bytes))
        
    def testAddWithNumbers(self):
        item, wi = self.simpleadd(name = '20y', content = '')
        wi = list(wi)
        self.assertEqual(len(wi),1)
        wi = wi[0]
        self.assertEqual(str(wi.word),'20y')
        
        
class TestSearchEngine(TestCase):
    tag = 'search'
    
    def testSimpleAdd(self):
        self.simpleadd()
        
    def testDoubleEntries(self):
        '''Test an item indexed twice'''
        engine = self.engine
        item,wi = self.simpleadd()
        wi = set((w.word for w in wi))
        session = engine.index_item(item)
        self.assertTrue(session)
        session.commit()
        wi2 = set((w.word for w in WordItem.objects.for_model(item)))
        # Lets get the words for item
        self.assertEqual(wi,wi2)
        
    def testSearchWords(self):
        self.simpleadd()
        words = self.engine.words('python gains')
        self.assertTrue(len(words)>=2)
        
    def testSearchModelSimple(self):
        item,_ = self.simpleadd()
        qs = Item.objects.search('python gains')
        self.assertEqual(qs.text,'python gains')
        q = qs.construct()
        self.assertEqual(q.keyword,'intersect')
        self.assertEqual(len(q),2)
        self.assertEqual(qs.count(),1)
        self.assertEqual(item,qs[0])
        
    def testSearchModel(self):
        item1,wi1 = self.simpleadd()
        item2,wi2 = self.simpleadd('pink',content='the dark side of the moon')
        item3,wi3 = self.simpleadd('queen',content='we will rock you')
        item4,wi4 = self.simpleadd('python',content='nothing here')
        qs = Item.objects.search('python')
        qc = qs.construct()
        self.assertEqual(len(qc),2)
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
        self.make_items(num=100)
        for item in Item.objects.query():
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
        words = list(Word.objects.query())
        item.delete()
        wis = WordItem.objects.filter(model_type = item.__class__)
        self.assertFalse(wis.count(),0)
        self.assertEqual(len(words),len(Word.objects.query()))
    
    