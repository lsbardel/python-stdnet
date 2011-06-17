import datetime
from stdnet import test
from stdnet.utils import populate, zip, range

from examples.models import SportAtDate, TestDateModel

NUM_DATES = 200

dates = populate('date',NUM_DATES,
                 start=datetime.date(2005,6,1),
                 end=datetime.date(2010,6,6))
sports = ['football','rugby','swimming','running','cycling']
groups = populate('choice',NUM_DATES,choice_from=sports)


class TestOrderByModel(test.TestCase):
    
    def setUp(self):
        self.orm.register(SportAtDate)
    
    def fill(self):
        with SportAtDate.transaction() as t:
            for n,d in zip(groups,dates):
                SportAtDate(name = n, dt = d).save(t)
                
    def testMeta(self):
        self.assertEqual(SportAtDate._meta.order_by.name,'dt')
        
    def testAdd(self):
        self.fill()
        self.assertEqual(SportAtDate.objects.all().count(),NUM_DATES)
        
        
class TestSort(test.TestCase):
    
    def setUp(self):
        self.orm.register(TestDateModel)
    
    def fill(self):
        with TestDateModel.transaction() as t:
            for n,d in zip(groups,dates):
                TestDateModel(name = n, dt = d).save(t)
        self.assertEqual(TestDateModel.objects.all().count(),NUM_DATES)
    
    def testSimpleFilter(self):
        self.fill()
        qs = TestDateModel.objects.all().order_by('dt')
        dt = None
        for obj in qs:
            if not dt:
                dt = obj.dt
            else:
                self.assertTrue(obj.dt>=dt)
                dt = obj.dt
        
    