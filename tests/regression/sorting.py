from datetime import date, datetime

from stdnet import test, QuerySetError, orm
from stdnet.utils import populate, zip, range

from examples.models import SportAtDate, SportAtDate2, Person,\
                             TestDateModel, Group

NUM_DATES = 200

dates = populate('date',NUM_DATES,
                 start=date(2005,6,1),
                 end=date(2010,6,6))

groups = populate('choice',NUM_DATES,
            choice_from=['football','rugby','swimming','running','cycling'])
persons = populate('choice',NUM_DATES,
            choice_from=['pippo','pluto','saturn','luca','josh','carl','paul'])
 
    
class TestSort(test.TestCase):
    '''Base class for sorting'''
    desc = False
    
    def fill(self):
        session = self.session()
        with session.begin():
            for p,n,d in zip(persons,groups,dates):
                session.add(self.model(person = p, name = n, dt = d))
        qs = session.query(self.model)
        self.assertEqual(qs.count(),NUM_DATES)
        return qs
    
    def checkOrder(self, qs, attr, desc = None):
        self.assertTrue(qs)
        desc = desc if desc is not None else self.desc
        at0 = qs[0].get_attr_value(attr)
        for obj in qs[1:]:
            at1 = obj.get_attr_value(attr)
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
    
        
class TestSortBy(TestSort,ExplicitOrderingMixin):
    '''Test the sort_by in a model without ordering meta attribute.
Pure explicit ordering.'''
    model = TestDateModel
    
    
class TestSortByForeignKeyField(TestSort):
    model = Person
    models = (Person,Group)
        
    def fill(self):
        session = self.session()
        with session.begin():
            for g in groups:
                session.add(Group(name = g))
                
        model = self.model
        gps = populate('choice', NUM_DATES, choice_from = session.query(Group))
        with session.begin():
            for p,g in zip(persons,gps):
                session.add(model(name = p, group = g))
        qs = session.query(model)
        self.assertEqual(qs.count(),NUM_DATES)
        return qs
    
    def testNameSortBy(self):
        self.checkOrder(self.fill().sort_by('name'),'name')
        
    def testNameSortByReversed(self):
        self.checkOrder(self.fill().sort_by('-name'),'name',True)
        
    def testSortByFK(self):
        qs = self.fill()
        qs = qs.sort_by('group__name')
        ordering = qs.ordering
        self.assertEqual(ordering.name,'group_id')
        self.assertEqual(ordering.nested.name,'name')
        self.assertEqual(ordering.model,qs.model)
        self.checkOrder(qs,'group__name')
        

class TestOrderingModel(TestSort):
    '''Test a model wich is always sorted by the ordering meta attribute.'''
    model = SportAtDate
    
    def testMeta(self):
        model = self.model
        self.assertTrue(model._meta.ordering)
        ordering = model._meta.ordering
        self.assertEqual(ordering.name,'dt')
        self.assertEqual(ordering.field.name,'dt')
        self.assertEqual(ordering.desc,self.desc)
    
    def testAdd(self):
        session = self.session()
        with session.begin():
            a = session.add(self.model(person='luca',name='football',
                                       dt=date.today()))
            b = session.add(self.model(person='luca',name='football',
                                       dt=date.today()))
        self.assertEqual(session.query(self.model).count(),2)
        
    def testSimple(self):
        self.checkOrder(self.fill(),'dt')
        
    def testFilter(self):
        # Require zdiffstore
        qs = self.fill().filter(name__in = ('football','rugby'))
        self.checkOrder(qs,'dt')
        
    def testExclude(self):
        # Require zdiffstore
        qs = self.fill().exclude(name='rugby')
        self.checkOrder(qs,'dt')
        
        
class TestOrderingModelDesc(TestOrderingModel):
    model = SportAtDate2
    desc = True
