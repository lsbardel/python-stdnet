"""Tests meta classes and corner cases of the library"""
import inspect
from datetime import datetime

from examples.data import FinanceTest, Fund, Instrument, Position
from examples.models import ComplexModel, SimpleModel

from stdnet import odm
from stdnet.odm import ModelType, model_iterator
from stdnet.utils import pickle, test


class TestInspectionAndComparison(FinanceTest):
    def test_simple(self):
        d = odm.model_to_dict(Instrument)
        self.assertFalse(d)
        inst = yield self.session().add(
            Instrument(name="erz12", type="future", ccy="EUR")
        )
        d = odm.model_to_dict(inst)
        self.assertTrue(len(d), 3)

    def testEqual(self):
        session = self.session()
        inst = yield session.add(Instrument(name="erm12", type="future", ccy="EUR"))
        id = inst.id
        b = yield self.query().get(id=id)
        self.assertEqual(b.id, id)
        self.assertTrue(inst == b)
        self.assertFalse(inst != b)
        f = yield session.add(Fund(name="bla", ccy="EUR"))
        self.assertFalse(inst == f)
        self.assertTrue(inst != f)

    def testNotEqual(self):
        session = self.session()
        inst = yield session.add(Instrument(name="erz22", type="future", ccy="EUR"))
        inst2 = yield session.add(Instrument(name="edz24", type="future", ccy="USD"))
        id = inst.id
        b = yield self.query().get(id=id)
        self.assertEqual(b.id, id)
        self.assertFalse(inst2 == b)
        self.assertTrue(inst2 != b)

    def testHash(self):
        """Test model instance hash"""
        inst = Instrument(name="erh12", type="future", ccy="EUR")
        h0 = hash(inst)
        self.assertTrue(h0)
        inst = yield self.session().add(inst)
        h = hash(inst)
        self.assertTrue(h)
        self.assertNotEqual(h, h0)

    def testmodelFromHash(self):
        m = odm.get_model_from_hash(Instrument._meta.hash)
        self.assertEqual(m, Instrument)

    def testUniqueId(self):
        """Test model instance unique id across different model"""
        inst = Instrument(name="erk12", type="future", ccy="EUR")
        self.assertRaises(inst.DoesNotExist, lambda: inst.uuid)
        yield self.session().add(inst)
        v = inst.uuid.split(".")  # <<model hash>>.<<instance id>>
        self.assertEqual(len(v), 2)
        self.assertEqual(v[0], inst._meta.hash)
        self.assertEqual(v[1], str(inst.id))

    def testModelValueError(self):
        self.assertRaises(ValueError, Instrument, bla="foo")
        self.assertRaises(ValueError, Instrument, name="bee", bla="foo")
        self.assertRaises(ValueError, Instrument, name="bee", bla="foo", foo="pippo")


class PickleSupport(test.TestCase):
    model = Instrument

    def testSimple(self):
        inst = yield self.session().add(
            Instrument(name="erz12", type="future", ccy="EUR")
        )
        p = pickle.dumps(inst)
        inst2 = pickle.loads(p)
        self.assertEqual(inst, inst2)
        self.assertEqual(inst.name, inst2.name)
        self.assertEqual(inst.type, inst2.type)
        self.assertEqual(inst.ccy, inst2.ccy)

    def testTempDictionary(self):
        session = self.session()
        inst = yield session.add(Instrument(name="erz17", type="future", ccy="EUR"))
        self.assertTrue("cleaned_data" in inst._dbdata)
        p = pickle.dumps(inst)
        inst2 = pickle.loads(p)
        self.assertFalse("cleaned_data" in inst2._dbdata)
        yield session.add(inst2)
        self.assertTrue("cleaned_data" in inst._dbdata)


class TestRegistration(test.TestCase):
    def testModelIterator(self):
        g = model_iterator("examples")
        self.assertTrue(inspect.isgenerator(g))
        d = list(g)
        self.assertTrue(d)
        for m in d:
            self.assertTrue(inspect.isclass(m))
            self.assertTrue(isinstance(m, ModelType))


class TestStdModelMethods(test.TestCase):
    model = SimpleModel

    def testClone(self):
        session = self.session()
        s = yield session.add(
            SimpleModel(code="pluto", group="planet", cached_data="blabla")
        )
        self.assertEqual(s.cached_data, b"blabla")
        id = self.assertEqualId(s, 1)
        c = s.clone()
        self.assertEqual(c.id, None)
        self.assertFalse(c.cached_data)

    def test_clear_cache_fields(self):
        fields = self.model._meta.dfields
        self.assertTrue(fields["timestamp"].as_cache)
        self.assertFalse(fields["timestamp"].required)
        self.assertFalse(fields["timestamp"].index)
        session = self.session()
        m = yield session.add(self.model(code="bla", timestamp=datetime.now()))
        self.assertTrue(m.timestamp)
        m.clear_cache_fields()
        self.assertEqual(m.timestamp, None)
        m2 = yield session.add(m)
        self.assertEqual(m.id, m2.id)
        m = yield self.query().get(id=m.id)
        self.assertEqual(m.timestamp, None)


class TestComplexModel(test.TestCase):
    model = ComplexModel

    def testJsonClear(self):
        session = self.session()
        m = yield session.add(
            self.model(name="bla", data={"italy": "rome", "england": "london"})
        )
        m = yield self.query().load_only("name").get(id=1)
        self.assertFalse(m.has_all_data)
        m.data = {"france": "paris"}
        yield session.add(m)
        m = yield self.query().get(id=1)
        self.assertEqual(
            m.data, {"italy": "rome", "england": "london", "france": "paris"}
        )
        self.assertEqual(m.data__italy, "rome")
        m.data = None
        yield session.add(m)
        m = yield self.query().get(id=1)
        self.assertEqual(m.data, {})
