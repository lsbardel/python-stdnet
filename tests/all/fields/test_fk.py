from examples.models import Group, Person

import stdnet
from stdnet import FieldError, odm
from stdnet.utils import test


class TestForeignKey(test.TestCase):
    models = (Person, Group)

    @classmethod
    def after_setup(cls):
        session = cls.mapper.session()
        with session.begin() as t:
            t.add(Group(name="bla"))
        yield t.on_result
        g = yield session.query(Group).get(name="bla")
        with session.begin() as t:
            t.add(Person(name="foo", group=g))
        yield t.on_result

    def testSimple(self):
        session = self.session()
        query = session.query(Person)
        yield self.async.assertEqual(query.count(), 1)
        p = yield query.get(name="foo")
        self.assertTrue(p.group_id)
        p.group = None
        self.assertEqual(p.group_id, None)

    def testOldRelatedNone(self):
        models = self.mapper
        p = yield models.person.get(name="foo")
        g = yield p.group
        self.assertTrue(g)
        self.assertEqual(g, p.group)
        self.assertEqual(g.id, p.group_id)
        p.group = None
        self.assertEqual(p.group_id, None)
        yield self.async.assertRaises(stdnet.FieldValueError, p.session.add, p)

    def testCoverage(self):
        self.assertRaises(FieldError, odm.ForeignKey, None)


class TestForeignKeyWrite(test.TestWrite):
    models = (Person, Group)

    def test_create(self):
        models = self.mapper
        group = yield models.group.new(name="quant")
        self.assertEqual(group.name, "quant")
        self.assertEqualId(group, 1)
        person = yield models.person.new(name="luca", group=group)
        self.assertEqualId(person, 1)
        self.assertEqual(group.id, person.group_id)
        self.assertEqual(group, person.group)
