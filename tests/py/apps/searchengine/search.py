'''Search engine application in `apps.searchengine`.'''
from stdnet import odm, QuerySetError, multi_async
from stdnet.utils import test, populate

from examples.wordsearch.models import Item, RelatedItem

from . import meta


class TestBigSearch(meta.TestCase):
    
    @classmethod
    def after_setup(cls):
        super(TestBigSearch, cls).after_setup()
        return cls.make_items(num=30, content=True)
    
    def test_backend(self):
        self.assertTrue(self.engine.backend)
        session = self.engine.session()
        self.assertEqual(session.backend, self.engine.backend)
    
    def testSearchWords(self):
        words = list(self.engine.words_from_text('python gains'))
        self.assertTrue(len(words)>=2)
        
    def test_items(self):
        wis = self.engine.worditems(Item)
        yield self.async.assertTrue(wis.count())
        
    def testBigSearch(self):
        sw = ' '.join(populate('choice', 1, choice_from=self.words))
        qs = yield self.query(Item).search(sw).all()
        self.assertTrue(qs)
        
    def testSearch(self):
        text = ' '.join(populate('choice', 1, choice_from=self.words))
        result = yield self.engine.search(text)
        self.assertTrue(result)
        
    def testNoWords(self):
        query = self.query(Item)
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
        queries = self.engine.search('')
        self.assertEqual(len(queries), 1)
        qs = yield queries[0].all()
        qs2 = yield self.engine.worditems().all()
        self.assertTrue(qs)
        self.assertEqual(set(qs), set(qs2))
        
    def test_bad_lookup(self):
        self.assertRaises(ValueError, self.engine.search,
                          'first second ', lookup='foo')
    
            
