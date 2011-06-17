import datetime

from stdnet import test, QuerySetError
from stdnet.utils import populate, zip, range

from examples.models import SportAtDate, TestDateModel

NUM_DATES = 200

dates = populate('date',NUM_DATES,
                 start=datetime.date(2005,6,1),
                 end=datetime.date(2010,6,6))
sports = ['football','rugby','swimming','running','cycling']
groups = populate('choice',NUM_DATES,choice_from=sports)


class TestSort(test.TestCase):
    
    def setUp(self):
        self.orm.register(self.model)
    
    def fill(self):
        model = self.model
        with model.transaction() as t:
            for n,d in zip(groups,dates):
                model(name = n, dt = d).save(t)
        qs = model.objects.all()
        self.assertEqual(qs.count(),NUM_DATES)
        return qs    
    
    def checkOrder(self, qs, desc = False):
        dt = None
        for obj in qs:
            if dt:
                if desc:
                    self.assertTrue(obj.dt<=dt)
                else:
                    self.assertTrue(obj.dt>=dt)
            dt = obj.dt


class TestOrderingModel(TestSort):
    model = SportAtDate
    
    def testMeta(self):
        self.assertTrue(SportAtDate._meta.ordering)
        ordering = SportAtDate._meta.ordering
        self.assertEqual(ordering.name,'dt')
        self.assertEqual(ordering.field.name,'dt')
        self.assertEqual(ordering.desc,False)
        
    def testSimple(self):
        self.checkOrder(self.fill())
        
        
class TestSortBy(TestSort):
    model = TestDateModel
    
    def testSimpleSortBy(self):
        self.checkOrder(self.fill().sort_by('dt'))
        
    def testSimpleSortByReversed(self):
        self.checkOrder(self.fill().sort_by('-dt'),True)
        
    def testSimpleSortError(self):
        qs = self.fill().sort_by('whaaaa')
        self.assertRaises(QuerySetError, lambda : list(qs))
        
    def testFilter(self):
        qs = self.fill().filter(name='rugby').sort_by('dt')
        self.assertTrue(qs)
        self.checkOrder(qs)
        for v in qs:
            self.assertEqual(v.name,'rugby')

