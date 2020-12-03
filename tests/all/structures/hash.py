from pulsar import multi_async

from stdnet import odm
from stdnet.utils import test

from .base import StructMixin


class TestHash(StructMixin, test.TestCase):
    structure = odm.HashTable
    name = "hashtable"

    def create_one(self):
        h = odm.HashTable()
        h["bla"] = "foo"
        h["pluto"] = 3
        return h

    def test_get_empty(self):
        d = self.empty()
        result = yield d.get("blaxxx", 3)
        self.assertEqual(result, 3)

    def test_pop(self):
        models = self.mapper
        d = models.register(self.create_one())
        session = models.session()
        with session.begin() as t:
            d = t.add(d)
            d["foo"] = "ciao"
        yield t.on_result
        yield self.async.assertEqual(d.size(), 3)
        yield self.async.assertEqual(d["foo"], "ciao")
        yield self.async.assertRaises(KeyError, d.pop, "blascd")
        yield self.async.assertEqual(d.pop("xxx", 56), 56)
        self.assertRaises(TypeError, d.pop, "xxx", 1, 2)
        yield self.async.assertEqual(d.pop("foo"), "ciao")
        yield self.async.assertEqual(d.size(), 2)

    def test_get(self):
        models = self.mapper
        d = models.register(self.structure())
        session = models.session()
        with session.begin() as t:
            d = t.add(d)
            d["baba"] = "foo"
            d["bee"] = 3
            self.assertEqual(len(d.cache.toadd), 2)
        yield t.on_result
        result = yield multi_async(
            (d["baba"], d.get("bee"), d.get("ggg"), d.get("ggg", 1))
        )
        self.assertEqual(result, ["foo", 3, None, 1])
        yield self.async.assertRaises(KeyError, lambda: d["gggggg"])

    def test_keys(self):
        models = self.mapper
        d = models.register(self.create_one())
        session = models.session()
        yield session.add(d)
        values = yield d.keys()
        self.assertEqual(set(("bla", "pluto")), set(values))

    def test_values(self):
        models = self.mapper
        d = models.register(self.create_one())
        session = models.session()
        yield session.add(d)
        values = yield d.values()
        self.assertEqual(set(("foo", 3)), set(values))

    def test_items(self):
        models = self.mapper
        d = models.register(self.create_one())
        session = models.session()
        yield session.add(d)
        values = yield d.items()
        data = {"bla": "foo", "pluto": 3}
        self.assertNotEqual(data, values)
        self.assertEqual(data, dict(values))
