from datetime import date, datetime

from stdnet import QuerySetError, odm
from stdnet.utils import test, populate, zip, range

from examples.data import data_generator
from examples.models import SportAtDate, SportAtDate2, Person,\
                             TestDateModel, Group

NUM_DATES = 200


class SortGenerator(data_generator):
    sizes = {'tiny': 20,
             'small': 100,
             'normal': 500,
             'big': 5000,
             'huge': 100000}
    
    def generate(self, **kwargs):
        self.dates = populate('date', self.size, start=date(2005,6,1),
                              end=date(2012,6,6))
        
        self.groups = populate('choice', self.size,
                choice_from=['football', 'rugby', 'swimming', 'running', 'cycling'])
        self.persons = populate('choice', self.size,
                    choice_from=['pippo', 'pluto', 'saturn', 'luca', 'josh', 'carl',
                                 'paul'])
 
    
class TestSort(test.TestCase):
    '''Base class for sorting'''
    desc = False
    
    @classmethod
    def after_setup(cls):
        cls.data = d = SortGenerator(cls.size)
        with cls.session().begin() as t:
            for p, n, d in zip(d.persons, d.groups, d.dates):
                t.add(cls.model(person=p, name=n, dt=d))
        return t.on_result
    
    def qs(self):
        return self.session().query(self.model)
    
    def checkOrder(self, qs, attr, desc=None):
        if hasattr(qs, 'all'):
            all = yield qs.all()
        else:
            all = qs
        self.assertTrue(all)
        desc = desc if desc is not None else self.desc
        at0 = qs[0].get_attr_value(attr)
        for obj in all[1:]:
            at1 = obj.get_attr_value(attr)
            if desc:
                self.assertTrue(at1<=at0)
            else:
                self.assertTrue(at1>=at0)
            at0 = at1
            

class ExplicitOrderingMixin(object):
    
    def test_size(self):
        qs = self.qs()
        yield self.async.assertEqual(qs.count(), len(self.data.dates))
        
    def testDateSortBy(self):
        return self.checkOrder(self.qs().sort_by('dt'), 'dt')
        
    def testDateSortByReversed(self):
        return self.checkOrder(self.qs().sort_by('-dt'),'dt',True)
        
    def testNameSortBy(self):
        return self.checkOrder(self.qs().sort_by('name'),'name')
        
    def testNameSortByReversed(self):
        return self.checkOrder(self.qs().sort_by('-name'),'name',True)
        
    def testSimpleSortError(self):
        qs = self.qs()
        self.assertRaises(QuerySetError, qs.sort_by, 'whaaaa')
        
    def testFilter(self):
        qs = self.qs().filter(name='rugby').sort_by('dt')
        yield self.checkOrder(qs, 'dt')
        for v in qs:
            self.assertEqual(v.name, 'rugby')

    def _slicingTest(self, attr, desc, start=0, stop=10, expected_len=10):
        p = '-' if desc else ''
        qs = self.qs().sort_by(p+attr)
        qs1 = yield qs[start:stop]
        self.assertEqual(len(qs1), expected_len)
        self.checkOrder(qs1, attr, desc)
        
    def testDateSlicing(self):
        return self._slicingTest('dt',False)
        
    def testDateSlicingDesc(self):
        return self._slicingTest('dt',True)
    
        
class TestSortBy(TestSort, ExplicitOrderingMixin):
    '''Test the sort_by in a model without ordering meta attribute.
Pure explicit ordering.'''
    model = TestDateModel
    
    
class TestSortByForeignKeyField(TestSort):
    model = Person
    models = (Person, Group)
        
    @classmethod
    def after_setup(cls):
        cls.data = d = SortGenerator(cls.size)
        session = cls.session()
        with session.begin() as t:
            for g in d.groups:
                t.add(Group(name=g))
        yield t.on_result
        groups = yield session.query(Group).all()
        gps = populate('choice', d.size, choice_from=groups)
        with session.begin() as t:
            for p, g in zip(d.persons, gps):
                t.add(cls.model(name=p, group=g))
        yield t.on_result
    
    def test_size(self):
        qs = self.qs()
        return self.async.assertEqual(qs.count(), len(self.data.dates))
        
    def testNameSortBy(self):
        return self.checkOrder(self.qs().sort_by('name'),'name')
        
    def testNameSortByReversed(self):
        return self.checkOrder(self.qs().sort_by('-name'),'name',True)
        
    def testSortByFK(self):
        qs = self.qs()
        qs = qs.sort_by('group__name')
        ordering = qs.ordering
        self.assertEqual(ordering.name, 'group_id')
        self.assertEqual(ordering.nested.name, 'name')
        self.assertEqual(ordering.model, qs.model)
        self.checkOrder(qs, 'group__name')
        

class TestOrderingModel(TestSort):
    '''Test a model which is always sorted by the ordering meta attribute.'''
    model = SportAtDate
    
    def testMeta(self):
        model = self.model
        self.assertTrue(model._meta.ordering)
        ordering = model._meta.ordering
        self.assertEqual(ordering.name, 'dt')
        self.assertEqual(ordering.field.name, 'dt')
        self.assertEqual(ordering.desc, self.desc)
        
    def testSimple(self):
        yield self.checkOrder(self.qs(),'dt')
        
    def testFilter(self):
        qs = self.qs().filter(name=('football','rugby'))
        return self.checkOrder(qs,'dt')
        
    def testExclude(self):
        qs = self.qs().exclude(name='rugby')
        return self.checkOrder(qs, 'dt')
        
        
class TestOrderingModelDesc(TestOrderingModel):
    model = SportAtDate2
    desc = True

