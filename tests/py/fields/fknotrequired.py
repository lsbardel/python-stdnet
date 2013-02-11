import stdnet
from stdnet import odm, FieldError
from stdnet.utils import test

from examples.models import Feed1, Feed2, CrossData 


class NonRequiredForeignKey(test.CleanTestCase):
    models = (Feed1, Feed2, CrossData)
    
    def setUp(self):
        self.register()
        
    def create_feeds(self):
        session = self.session()
        with session.begin():
            session.add(Feed1(name='bla'))
            session.add(Feed1(name='foo'))
            
    def create_feeds_with_data(self):
        self.create_feeds()
        feed = Feed1.objects.get(name='bla')
        feed.live = CrossData(name='live', data={'pv': 30, 'delta': 40}).save()
        feed.save()
        feed = Feed1.objects.get(name='foo')
        feed.live = CrossData(name='live', data={'pv': 40, 'delta': 20}).save()
        feed.save()
        
    def test_nodata(self):
        self.create_feeds()
        session = self.session()
        feeds = session.query(Feed1).all()
        for feed in feeds:
            self.assertFalse(feed.live)
            self.assertFalse(feed.prev)
    
    def test_width_data(self):
        self.create_feeds_with_data()
        feed = Feed1.objects.get(name='bla')
        self.assertEqual(feed.live.data__pv, 30)
        
    def test_load_only(self):
        self.create_feeds_with_data()
        feed = Feed1.objects.query().load_only('live__data__pv').get(name='bla')
        self.assertFalse(feed.live.has_all_data)
        self.assertEqual(feed.live.data, {'pv': 30})
        
    def test_filter(self):
        self.create_feeds_with_data()
        feeds = Feed1.objects.filter(live__data__pv__gt=35)
        self.assertEqual(feeds.count(), 1)
        
    def test_delete(self):
        self.create_feeds_with_data()
        CrossData.objects.filter(name='live').delete()
        feeds = Feed1.objects.query()
        self.assertEqual(feeds.count(), 2)
        for feed in feeds:
            self.assertFalse(feed.live)
            self.assertFalse(feed.live_id)
            self.assertFalse(feed.prev)
            self.assertFalse(feed.prev_id)
            
    def test_load_related(self):
        self.create_feeds()
        for feed in Feed1.objects.query().load_related('live', 'id'):
            self.assertEqual(feed.live, None)