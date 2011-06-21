import datetime

from stdnet import test, QuerySetError
from stdnet.utils import populate, zip, range

from examples.models import SportAtDate, SportAtDate2, TestDateModel

NUM_DATES = 200

dates = populate('date',NUM_DATES,
                 start=datetime.date(2005,6,1),
                 end=datetime.date(2010,6,6))

groups = populate('choice',NUM_DATES,
                  choice_from=['football','rugby','swimming','running','cycling'])
persons = populate('choice',NUM_DATES,
                   choice_from=['pippo','pluto','saturn','luca','josh','carl','paul'])
 
    
class TestSort(test.TestCase):
    desc = False
    
    def setUp(self):
        self.orm.register(self.model)
    
    def fill(self):
        model = self.model
        with model.transaction() as t:
            for p,n,d in zip(persons,groups,dates):
                model(person = p, name = n, dt = d).save(t)
        qs = model.objects.all()
        self.assertEqual(qs.count(),NUM_DATES)
        return qs    
    
    def checkOrder(self, qs, desc = None):
        desc = desc if desc is not None else self.desc
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
        model = self.model
        self.assertTrue(model._meta.ordering)
        ordering = model._meta.ordering
        self.assertEqual(ordering.name,'dt')
        self.assertEqual(ordering.field.name,'dt')
        self.assertEqual(ordering.desc,self.desc)
        
    def testSimple(self):
        self.checkOrder(self.fill())
        
    def testExclude(self):
        qs = self.fill().exclude(name='rugby')
        self.checkOrder(qs)
        

class TestOrderingModel2(TestOrderingModel):
    model = SportAtDate2
    desc = True
    
        
class TestSortBy(TestSort):
    model = TestDateModel
    
    def testSimpleSortBy(self):
        self.checkOrder(self.fill().sort_by('dt'))
        
    def testSimpleSortByReversed(self):
        self.checkOrder(self.fill().sort_by('-dt'),True)
        
    def testSimpleSortError(self):
        qs = self.fill()
        self.assertRaises(QuerySetError, qs.sort_by, 'whaaaa')
        
    def testFilter(self):
        qs = self.fill().filter(name='rugby').sort_by('dt')
        self.assertTrue(qs)
        self.checkOrder(qs)
        for v in qs:
            self.assertEqual(v.name,'rugby')

