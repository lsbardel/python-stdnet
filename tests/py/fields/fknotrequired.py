from pulsar import multi_async

import stdnet
from stdnet import odm, FieldError
from stdnet.utils import test

from examples.models import Feed1, Feed2, CrossData 


class NonRequiredForeignKey(test.TestCase):
    models = (Feed1, Feed2, CrossData)
        
    def create_feeds(self, *names):
        session = self.session()
        with self.mapper.session().begin() as t:
            for name in names:
                t.add(Feed1(name=name))
        return t.on_result
            
    def create_feeds_with_data(self, *names, **kwargs):
        models = self.mapper
        yield self.create_feeds(*names)
        all = yield models.feed1.filter(name=names).all()
        params = {'pv': 30, 'delta': 40, 'name': 'live'}
        params.update(kwargs)
        name = params.pop('name')
        with models.session().begin() as t:
            for feed in all:
                feed.live = yield models.crossdata.new(name=name, data=params)
                t.add(feed)
        yield t.on_result
        
    def test_nodata(self):
        yield self.create_feeds('bla', 'foo')
        session = self.session()
        feeds = yield session.query(Feed1).filter(name=('bla', 'foo')).all()
        for feed in feeds:
            live, prev = yield multi_async((feed.live, feed.prev))
            self.assertFalse(live)
            self.assertFalse(prev)
    
    def test_width_data(self):
        yield self.create_feeds_with_data('test1')
        feed = yield Feed1.objects.get(name='test1')
        live = yield feed.live
        self.assertEqual(live.data__pv, 30)
        
    def test_load_only(self):
        yield self.create_feeds_with_data('test2')
        feed = yield Feed1.objects.query().load_only('live__data__pv').get(name='test2')
        self.assertFalse(feed.live.has_all_data)
        self.assertEqual(feed.live.data, {'pv': 30})
      
    def test_filter(self):
        yield self.create_feeds_with_data('test3', pv=400)
        feeds = Feed1.objects.filter(live__data__pv__gt=300)
        yield self.async.assertEqual(feeds.count(), 1)
        
    def test_delete(self):
        yield self.create_feeds_with_data('test4', 'test5', name='pippo')
        yield CrossData.objects.filter(name='pippo').delete()
        feeds = yield Feed1.objects.query().filter(name=('test4', 'test5')).all()
        self.assertEqual(len(feeds), 2)
        for feed in feeds:
            live = yield feed.live
            prev = yield feed.prev
            self.assertFalse(live)
            self.assertFalse(feed.live_id)
            self.assertFalse(prev)
            self.assertFalse(feed.prev_id)
            
    def test_load_related(self):
        models = self.mapper
        yield self.create_feeds('jkjkjk')
        feed = yield models.feed1.query().load_related('live', 'id').get(name='jkjkjk')
        self.assertEqual(feed.live, None)

    def test_load_only_missing_related(self):
        '''load_only on a related field which is missing.'''
        models = self.mapper
        yield self.create_feeds('ooo', 'ooo2')
        qs = yield models.feed1.query().load_only('live__pv').filter(name__startswith='ooo')
        yield self.async.assertEqual(qs.count(), 2)
        qs = yield qs.all()
        for feed in qs:
            self.assertEqual(feed.live, None)
       
    def test_load_only_some_missing_related(self):
        '''load_only on a related field which is missing.'''
        yield self.create_feeds_with_data('aaa1', 'aaa2', name='palo')
        qs = yield Feed1.objects.query().filter(name__startswith='aaa')\
                                        .load_only('name', 'live__data__pv').all()
        self.assertEqual(len(qs), 2)
        for feed in qs:
            self.assertEqual(feed.live.data, {'pv': 30})

    def test_has_attribute(self):
        yield self.create_feeds_with_data('bbba', 'bbbc')
        qs = yield Feed1.objects.query().filter(name__startswith='bbb')\
                                .load_only('name', 'live__data__pv').all()
        self.assertEqual(len(qs), 2)
        for feed in qs:
            name = feed.get_attr_value('name')
            self.assertTrue(name.startswith('bbb'))
            self.assertEqual(feed.get_attr_value('live__data__pv'), 30)
            self.assertRaises(AttributeError, feed.get_attr_value, 'a__b')
                 
    def test_load_related_when_deleted(self):
        '''Use load_related on foreign key which was deleted.'''
        yield self.create_feeds_with_data('ccc1')
        feed = yield Feed1.objects.get(name='ccc1')
        live = yield feed.live
        self.assertTrue(feed.live)
        self.assertEqual(feed.live.id, feed.live_id)
        # Now we delete the feed
        yield feed.live.delete()
        # we still have a reference to it
        self.assertTrue(feed.live_id)
        self.assertTrue(feed.live)
        #
        feed = yield Feed1.objects.get(name='ccc1')
        live = yield feed.live
        self.assertFalse(feed.live)
        self.assertFalse(feed.live_id)
        #
        feed = yield Feed1.objects.query().load_related('live').get(name='ccc1')
        self.assertFalse(feed.live)
        self.assertFalse(feed.live_id)
        
    def test_sort_by_missing_fk_data(self):
        yield self.create_feeds('ddd1', 'ddd2')
        query = self.session().query(Feed1).filter(name__startswith='ddd')
        feed1s = yield query.sort_by('live').all()
        feed2s = yield query.sort_by('live__data__pv').all()
        self.assertEqual(len(feed1s), 2)
        self.assertEqual(len(feed2s), 2)
        