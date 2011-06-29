import datetime

from stdnet import test, QuerySetError, orm
from stdnet.utils import populate, zip, range

from examples.models import SportAtDate, SportAtDate2, Person, TestDateModel, Group

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
    
    def checkOrder(self, qs, attr, desc = None):
        attrs = attr.split(orm.JSPLITTER)
        self.assertTrue(qs)
        desc = desc if desc is not None else self.desc
        attr = attrs[0]
        at0 = getattr(qs[0],attr)
        for obj in qs[1:]:
            at1 = getattr(obj,attr)
            if desc:
                self.assertTrue(at1<=at0)
            else:
                self.assertTrue(at1>=at0)
            at0 = at1
            

class ExplicitOrderingMixin(object):
    
    def testDateSortBy(self):
        self.checkOrder(self.fill().sort_by('dt'),'dt')
        
    def testDateSortByReversed(self):
        self.checkOrder(self.fill().sort_by('-dt'),'dt',True)
        
    def testNameSortBy(self):
        self.checkOrder(self.fill().sort_by('name'),'name')
        
    def testNameSortByReversed(self):
        self.checkOrder(self.fill().sort_by('-name'),'name',True)
        
    def testSimpleSortError(self):
        qs = self.fill()
        self.assertRaises(QuerySetError, qs.sort_by, 'whaaaa')
        
    def testFilter(self):
        qs = self.fill().filter(name='rugby').sort_by('dt')
        self.checkOrder(qs,'dt')
        for v in qs:
            self.assertEqual(v.name,'rugby')

    def _slicingTest(self, attr, desc, start = 0, stop = 10,
                     expected_len = 10):
        p = '-' if desc else ''
        qs = self.fill().sort_by(p+attr)
        qs1 = qs[start:stop]
        self.assertEqual(len(qs1),expected_len)
        self.checkOrder(qs1,attr,desc)
        
    def testDateSlicing(self):
        self._slicingTest('dt',False)
        
    def testDateSlicingDesc(self):
        self._slicingTest('dt',True)


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
        self.checkOrder(self.fill(),'dt')
        
    def testExclude(self):
        qs = self.fill().exclude(name='rugby')
        self.checkOrder(qs,'dt')
        

class TestOrderingModel2(TestOrderingModel):
    model = SportAtDate2
    desc = True
    
        
class TestSortBy(TestSort,ExplicitOrderingMixin):
    '''Test the sort_by in a model without ordering meta attribute.
Pure explicit ordering.'''
    model = TestDateModel
    
    
class TestSortByForeignKeyField(TestSort):
    model = Person
    
    def setUp(self):
        self.orm.register(self.model)
        self.orm.register(Group)
        
    def fill(self):
        with Group.transaction() as t:
            for g in groups:
                Group(name = g).save(t)
                
        model = self.model
        gps = populate('choice',NUM_DATES,choice_from = Group.objects.all())
        with model.transaction() as t:
            for p,g in zip(persons,gps):
                model(name = p, group = g).save(t)
        qs = model.objects.all()
        self.assertEqual(qs.count(),NUM_DATES)
        return qs
    
    def testNameSortBy(self):
        self.checkOrder(self.fill().sort_by('name'),'name')
        
    def testNameSortByReversed(self):
        self.checkOrder(self.fill().sort_by('-name'),'name',True)
        
    #def testSortByFK(self):
    #    self.checkOrder(self.fill().sort_by('group__name'),'group__name')
        