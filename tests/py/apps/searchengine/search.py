'''Search engine application in `apps.searchengine`.'''
from stdnet import odm
from stdnet.utils import test, populate

from examples.wordsearch.models import Item, RelatedItem

from .meta import SearchMixin


class TestBigSearch(SearchMixin, test.TestCase):
    
    def setUp(self):
        self.mapper.set_search_engine(self.make_engine())
        self.mapper.search_engine.register(Item, ('related',))
        self.mapper.search_engine.register(RelatedItem)
        return self.make_items(num=30, content=True)
    
    def test_session(self):
        models = self.mapper
        self.assertFalse(models.search_engine.backend)
        session = models.search_engine.session()
        self.assertEqual(session.router, models)
    
    def testSearchWords(self):
        engine = self.mapper.search_engine
        words = list(engine.words_from_text('python gains'))
        self.assertTrue(len(words)>=2)
        
    def test_items(self):
        engine = self.mapper.search_engine
        wis = engine.worditems(Item)
        yield self.async.assertTrue(wis.count())
        
    def testBigSearch(self):
        models = self.mapper
        sw = ' '.join(populate('choice', 1, choice_from=self.words))
        qs = yield models.item.search(sw).all()
        self.assertTrue(qs)
        
    def testSearch(self):
        engine = self.mapper.search_engine
        text = ' '.join(populate('choice', 1, choice_from=self.words))
        result = yield engine.search(text)
        self.assertTrue(result)
        
    def testNoWords(self):
        models = self.mapper
        query = models.item.query()
        q1 = yield query.search('').all()
        all = yield query.all()
        self.assertTrue(q1)
        self.assertEqual(set(q1), set(all))
        
    def testInSearch(self):
        sw = ' '.join(populate('choice', 5, choice_from=self.words))
        query = self.session().query(Item)
        res1 = yield query.search(sw, engine=self.engine).all()
        res2 = yield query.search(sw, lookup='in',  engine=self.engine).all()
        self.assertTrue(res2)
        self.assertTrue(len(res1) < len(res2))
        
    def testEmptySearch(self):
        engine = self.mapper.search_engine
        queries = engine.search('')
        self.assertEqual(len(queries), 1)
        qs = yield queries[0].all()
        qs2 = yield engine.worditems().all()
        self.assertTrue(qs)
        self.assertEqual(set(qs), set(qs2))
        
    def test_bad_lookup(self):
        engine = self.mapper.search_engine
        self.assertRaises(ValueError, engine.search,
                          'first second ', lookup='foo')
    
            
