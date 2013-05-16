'''Test query.get_field method for obtaining a single field from a query.'''
from stdnet.utils import test, zip, is_string

from examples.models import Instrument, ObjectAnalytics,\
                             AnalyticData, Group, Fund
from examples.data import FinanceTest, INSTS_TYPES, CCYS_TYPES


class TestInstrument(FinanceTest):

    @classmethod
    def after_setup(cls):
        yield cls.data.create(cls)

    def testName(self):
        session = self.session()
        all = yield session.query(self.model).all()
        qb = dict(((i.name,i) for i in all))
        qs = session.query(self.model).get_field('name')
        self.assertEqual(qs._get_field, 'name')
        result = yield qs.all()
        self.assertTrue(result)
        for r in result:
            self.assertTrue(is_string(r))
            qb.pop(r)
        self.assertFalse(qb)

    def testId(self):
        session = self.session()
        qs = session.query(self.model).get_field('id')
        self.assertEqual(qs._get_field, 'id')
        result = yield qs.all()
        self.assertTrue(result)
        for r in result:
            self.assertTrue(isinstance(r, int))


class TestRelated(FinanceTest):

    @classmethod
    def after_setup(cls):
        return cls.data.makePositions(cls)

    def testInstrument(self):
        models = self.mapper
        qs = models.position.query().get_field('instrument')
        self.assertEqual(qs._get_field, 'instrument')
        result = yield qs.all()
        self.assertTrue(result)
        for r in result:
            self.assertTrue(type(r), int)

    def testFilter(self):
        models = self.mapper
        qs = models.position.query().get_field('instrument')
        qi = models.instrument.filter(id=qs)
        inst = yield qi.all()
        ids = yield qs.all()
        self.assertTrue(inst)
        self.assertTrue(len(ids) >= len(inst))
        idset = set(ids)
        self.assertEqual(len(idset), len(inst))
        self.assertEqual(idset, set((i.id for i in inst)))


class generator(test.DataGenerator):
    sizes = {'tiny': (2,10),
             'small': (5,30),
             'normal': (10,50),
             'big': (30,200),
             'huge': (100,1000)}

    def generate(self):
        group_len, obj_len = self.size
        self.inames = self.populate('string', obj_len, min_len=5, max_len=20)
        self.itypes = self.populate('choice', obj_len, choice_from=INSTS_TYPES)
        self.iccys = self.populate('choice', obj_len, choice_from=CCYS_TYPES)
        self.gnames = self.populate('string', group_len, min_len=5, max_len=20)

    def create(self, test):
        session = test.session()
        with session.begin() as t:
            for name, typ, ccy in zip(self.inames, self.itypes, self.iccys):
                t.add(Instrument(name=name, type=typ, ccy=ccy))
            for name in self.gnames:
                t.add(Group(name=name))
            for name, ccy in zip(self.inames, self.iccys):
                t.add(Fund(name=name, ccy=ccy))
        yield t.on_result
        iall = yield test.session().query(Instrument).load_only('id').all()
        fall = yield test.session().query(Fund).load_only('id').all()
        with session.begin() as t:
            for i in iall:
                t.add(ObjectAnalytics(model_type=Instrument, object_id=i.id))
            for i in fall:
                t.add(ObjectAnalytics(model_type=Fund, object_id=i.id))
        yield t.on_result
        obj_len = self.size[1]
        groups = yield session.query(Group).all()
        objs = yield session.query(ObjectAnalytics).all()
        groups = self.populate('choice', obj_len, choice_from=groups)
        objs = self.populate('choice', obj_len, choice_from=objs)
        with test.session().begin() as t:
            for g, o in zip(groups, objs):
                t.add(AnalyticData(group=g, object=o))
        yield t.on_result


class TestModelField(test.TestWrite):
    '''Test the get_field method when applied to ModelField'''
    models = (ObjectAnalytics, AnalyticData, Group, Instrument, Fund)
    data_cls = generator

    def setUp(cls):
        return cls.data.create(cls)

    def testLoad(self):
        session = self.session()
        q = session.query(ObjectAnalytics)\
                   .filter(model_type=Instrument).get_field('id')
        i = session.query(Instrument).filter(id=q)
        yield self.async.assertEqual(i.count(), session.query(Instrument).count())

    def testLoadMissing(self):
        models = self.mapper
        yield models.instrument.filter(id=(1,2,3)).delete()
        q = models.objectanalytics.filter(model_type=Instrument).get_field('id')
        i = yield models.instrument.filter(id=q).all()
        self.assertTrue(i)

    def testUnion(self):
        models = self.mapper
        def query():
            model_permissions = models.objectanalytics.filter(id=(1,2,3))
            objects = models.analyticdata.exclude(object__model_type=Instrument)\
                                         .get_field('object')
            return model_permissions.union(objects).all()
        result1 = yield query()
        self.assertTrue(result1)
        # Now remove some instruments
        yield models.instrument.filter(id=(1,2,3)).delete()
        #
        result2 = yield query()
        self.assertTrue(result2)
