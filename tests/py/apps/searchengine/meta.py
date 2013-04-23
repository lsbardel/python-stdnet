from random import randint

from stdnet import odm, QuerySetError, multi_async
from stdnet.utils import test
from stdnet.odm.search import UpdateSE
from stdnet.utils import test, to_string, range, populate
from stdnet.apps.searchengine import SearchEngine, processors

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
    
    
class TestCase(test.TestCase):
    '''Mixin for testing the search engine. No tests implemented here,
just registration and some utility functions. All search-engine tests
below will derive from this class.'''
    multipledb = 'redis'
    metaphone = True
    stemming = True
    
    @classmethod
    def after_setup(cls):
        cls.engine = cls.make_engine()
        cls.engine.register(Item, ('related',))
        cls.engine.register(RelatedItem)
    
    @classmethod
    def make_engine(cls):
        return SearchEngine(metaphone=cls.metaphone, stemming=cls.stemming,
                            backend=cls.backend)
    
    def query(self, model):
        query = self.session().query(model, searchengine=self.engine)
        self.assertEqual(query.searchengine, self.engine)
        self.assertFalse(model.searchengine)
        return query
    
    def make_item(self, name='python', counter=10, content=None, related=None):
        content = content if content is not None else python_content
        with self.session().begin() as t:
            item = t.add(Item(name=name, counter=counter,
                              content=content, related=related))
        yield t.on_result
        yield item
    
    @classmethod
    def make_items(cls, num=30, content=False, related=None):
        '''Bulk creation of Item for testing search engine. Return a set
of words which have been included in the Items.'''
        names = populate('choice', num, choice_from=basic_english_words)
        session = cls.session()
        words = set()
        if content:
            contents = WORDS_GROUPS(num)
        else:
            contents = ['']*num
        with session.begin() as t:
            for name, content in zip(names, contents):
                if len(name) > 3:
                    words.add(name)
                    if content:
                        words.update(content.split())
                    t.add(Item(name=name, counter=randint(0,10),
                               content=content, related=related))
        yield t.on_result
        cls.words = words
        yield cls.words
    
    def simpleadd(self, name='python', counter=10, content=None, related=None):
        item = yield self.make_item(name, counter, content, related)
        wis = yield self.engine.worditems(item).all()
        self.assertTrue(wis)
        session = self.session()
        objets = yield multi_async((wi.object(session) for wi in wis))
        for object in objets:
            self.assertEqual(object, item)
        yield item, wis


class TestMeta(TestCase):
    '''Test internal functions, not the API.'''
    def testSplitting(self):
        eg = SearchEngine(metaphone=False, stemming=False)
        self.assertEqual(list(eg.words_from_text('bla-ciao+pippo')),\
                         ['bla','ciao','pippo'])
        self.assertEqual(list(eg.words_from_text('bla.-ciao:;pippo')),\
                         ['bla','ciao','pippo'])
        self.assertEqual(list(eg.words_from_text('  bla ; @ciao ;:`')),\
                         ['bla','ciao'])
        self.assertEqual(list(eg.words_from_text('bla bla____bla')),\
                         ['bla','bla','bla'])
        
    def testSplitters(self):
        eg = SearchEngine(splitters=False)
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
        self.assertTrue(RelatedItem in self.engine.REGISTERED_MODELS)
        self.assertFalse(SimpleModel in self.engine.REGISTERED_MODELS)
        self.assertEqual(self.engine.REGISTERED_MODELS[Item].related,
                         ('related',))
        self.assertEqual(self.engine.REGISTERED_MODELS[RelatedItem].related, ())
        
    def testNoSearchEngine(self):
        query = self.session().query(SimpleModel)
        qs = query.search('bla')
        self.assertRaises(QuerySetError, qs.all)
        

class TestCoverageBaseClass(test.TestCase):
    
    def testAbstracts(self):
        e = odm.SearchEngine()
        self.assertRaises(NotImplementedError, e.search, 'bla')
        self.assertRaises(NotImplementedError, e.search_model, None, 'bla')
        self.assertRaises(NotImplementedError, e.flush)
        self.assertRaises(NotImplementedError, e.add_item, None, None, None)
        self.assertRaises(NotImplementedError, e.remove_item, None, None, None)
        self.assertRaises(NotImplementedError, e.session)
        self.assertEqual(e.split_text('ciao luca'), ['ciao','luca'])
    
    def testItemFieldIterator(self):
        e = odm.SearchEngine()
        self.assertRaises(ValueError, e.item_field_iterator, None)
        u = UpdateSE(e)
        self.assertEqual(u.se, e)