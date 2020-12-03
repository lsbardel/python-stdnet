"""tests for odm.ListField"""
from examples.models import SimpleList

from stdnet import StructureFieldError
from stdnet.utils import test, to_string, zip

from .struct import MultiFieldMixin, StringData


class TestListField(MultiFieldMixin, test.TestCase):
    model = SimpleList
    attrname = "names"

    def adddata(self, li):
        """Add elements to a list without using transactions."""
        with li.session.begin():
            names = li.names
            for elem in self.data.names:
                names.push_back(elem)
        self.assertEqual(li.names.size(), len(self.data.names))

    def test_push_back_pop_back(self):
        li = SimpleList()
        self.assertEqual(li.id, None)
        li = yield self.session().add(li)
        yield self.adddata(li)
        # pop back one by one
        results = []
        names = list(reversed(self.data.names))
        for elem in names:
            results.append(li.names.pop_back())
        yield self.multi_async(results)
        self.assertEqual(results, names)
        yield self.async.assertEqual(li.names.size(), 0)

    def test_push_back(self):
        models = self.mapper
        li = yield models.simplelist.new()
        with li.session.begin() as t:
            names = li.names
            for elem in self.data.names:
                names.push_back(elem)
        yield t.on_result
        all = yield names.items()
        self.assertEqual(len(all), len(self.data.names))
        for el, ne in zip(self.data.names, all):
            self.assertEqual(el, ne)

    def testPushNoSave(self):
        """Push a new value to a list field should rise an error if the object
        is not saved on databse."""
        obj = self.model()
        push_back = lambda: obj.names.push_back("this should fail")
        push_front = lambda: obj.names.push_front("this should also fail")
        self.assertRaises(StructureFieldError, push_back)
        self.assertRaises(StructureFieldError, push_front)

    def test_items(self):
        session = self.session()
        li = yield session.add(SimpleList())
        yield self.adddata(li)
        size = yield li.names.size()
        self.assertEqual(size, len(self.data.names))
        all = yield li.names.items()
        self.assertEqual(all, self.data.names)
        self.assertEqual(all, li.names.cache.cache)


class TestRedisListField(test.TestCase):
    multipledb = ["redis"]
    model = SimpleList
    data_cls = StringData

    def testPushFront(self):
        session = self.session()
        li = yield session.add(SimpleList())
        names = li.names
        self.assertEqual(li.session, session)
        with session.begin() as t:
            for elem in reversed(self.data.names):
                names.push_front(elem)
        yield t.on_result
        all = yield names.items()
        for el, ne in zip(self.data.names, all):
            self.assertEqual(el, ne)

    def test_push_front_pop_front(self):
        session = self.session()
        li = yield session.add(SimpleList())
        names = li.names
        self.assertEqual(li.session, session)
        with session.begin() as t:
            for elem in reversed(self.data.names):
                names.push_front(elem)
        yield t.on_result
        size = yield names.size()
        self.assertEqual(size, len(self.data.names))
        #
        results = []
        for elem in self.data.names:
            results.append(li.names.pop_front())
        yield self.multi_async(results)
        self.assertEqual(results, self.data.names)
        size = yield names.size()
        self.assertEqual(size, 0)
