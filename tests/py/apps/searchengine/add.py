from stdnet.utils import test

from .meta import Item, RelatedItem, SearchMixin


class TestSearchAddToEngine(SearchMixin, test.TestWrite):
          
    def testSimpleAdd(self):
        return self.simpleadd()
    
    def testDoubleEntries(self):
        '''Test an item indexed twice'''
        item, wi = yield self.simpleadd()
        wi = set((w.word for w in wi))
        yield item.save()
        items = yield self.engine.worditems(item).all()
        wi2 = set((w.word for w in items))
        self.assertEqual(wi, wi2)    
        
    def testSearchWords(self):
        yield self.simpleadd()
        words = list(self.engine.words_from_text('python gains'))
        self.assertTrue(len(words) >= 2)
        
    def testSearchModelSimple(self):
        item, _ = yield self.simpleadd()
        qs = self.query(Item).search('python gains')
        self.assertEqual(qs.text, ('python gains',None))
        q = qs.construct()
        self.assertEqual(q.keyword, 'intersect')
        self.assertEqual(len(q), 4)
        qs = yield qs.all()
        self.assertEqual(len(qs), 1)
        self.assertEqual(item, qs[0])
        
    def testSearchModel(self):
        yield self.simpleadd()
        yield self.simpleadd('pink', content='the dark side of the moon')
        yield self.simpleadd('queen', content='we will rock you')
        yield self.simpleadd('python', content='nothing here')
        qs = self.query(Item).search('python')
        qc = qs.construct()
        self.assertEqual(len(qc),3)
        self.assertEqual(qc.keyword, 'intersect')
        yield self.async.assertEqual(qs.count(), 2)
        qs = yield self.query(Item).search('python learn').all()
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0].name, 'python')
        
    def testRelatedModel(self):
        session = self.session()
        with session.begin() as t:
            r = t.add(RelatedItem(name='planet earth is wonderful'))
        yield t.on_result
        yield self.simpleadd('king', content='england')
        yield self.simpleadd('nothing', content='empty', related=r)
        qs = self.query(Item).search('planet')
        qc = qs.construct()
        self.assertEqual(len(qc), 2)
        self.assertEqual(qc.keyword, 'intersect')
        yield self.async.assertEqual(qs.count(), 1)
        
    def testFlush(self):
        yield self.make_items()
        yield self.engine.flush()
        yield self.async.assertFalse(self.engine.worditems().count())
        
    def testDelete(self):
        item = yield self.make_item()
        words = yield self.engine.worditems().all()
        self.assertTrue(words)
        yield item.delete()
        wis = self.engine.worditems(Item)
        yield self.async.assertFalse(wis.count(), 0)
        
    def testReindex(self):
        yield self.make_items()
        wis1 = yield self.engine.worditems().all()
        self.assertTrue(wis1)
        yield self.engine.reindex()
        wis2 = yield self.engine.worditems().all()
        self.assertTrue(wis1)
        self.assertEqual(set(wis1), set(wis2))
        
    def test_skip_indexing_when_missing_fields(self):
        session = self.session()
        item, wis = yield self.simpleadd()
        obj = yield self.query(Item).load_only('id').get(id=item.id)
        yield obj.save()
        wis2 = yield self.engine.worditems(obj).all()
        self.assertEqual(wis, wis2)
        
    def testAddWithNumbers(self):
        item, wi = yield self.simpleadd(name='20y', content='')
        wi = list(wi)
        self.assertEqual(len(wi),1)
        wi = wi[0]
        self.assertEqual(str(wi.word),'20y')


class TestCoverage(SearchMixin, test.TestWrite):
    
    @classmethod
    def make_engine(cls):
        eg = meta.SearchEngine(metaphone=False)
        eg.add_word_middleware(meta.processors.metaphone_processor)
        return eg
    
    def testAdd(self):
        item, wi = yield self.simpleadd('pink',
                                  content='the dark side of the moon 10y')
        wi = set((str(w.word) for w in wi))
        self.assertEqual(len(wi), 4)
        self.assertFalse('10y' in wi)

    def testRepr(self):
        item, wi = yield self.simpleadd('pink',
                                  content='the dark side of the moon 10y')
        for w in wi:
            self.assertEqual(str(w), str(w.word))