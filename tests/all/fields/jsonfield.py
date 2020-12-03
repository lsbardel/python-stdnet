import json
import os
import time
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from random import choice, randint, random

from examples.models import Role, Statistics, Statistics3

import stdnet
from stdnet.utils import date2timestamp, ispy3k, range, test, to_string, unichr, zip
from stdnet.utils.populate import populate


class make_random(object):
    rtype = ["number", "list", None] + ["dict"] * 3

    def __init__(self):
        self.count = 0

    def make(self, size=5, maxsize=10, nesting=1, level=0):
        keys = populate(size=size)
        if level:
            keys.append("")
        for key in keys:
            t = choice(self.rtype) if level else "dict"
            if nesting and t == "dict":
                yield key, dict(
                    self.make(
                        size=randint(0, maxsize),
                        maxsize=maxsize,
                        nesting=nesting - 1,
                        level=level + 1,
                    )
                )
            else:
                if t == "list":
                    v = [random() for i in range(10)]
                elif t == "number":
                    v = random()
                elif t == "dict":
                    v = random()
                else:
                    v = t
                yield key, v


class TestJsonField(test.TestCase):
    models = [Statistics, Role]

    def test_default(self):
        models = self.mapper
        a = Statistics(dt=date.today())
        self.assertEqual(a.data, {})
        yield models.add(a)
        self.assertEqual(a.data, {})
        a = yield models.statistics.get(id=a.id)
        self.assertEqual(a.data, {})

    def testMetaData(self):
        field = Statistics._meta.dfields["data"]
        self.assertEqual(field.type, "json object")
        self.assertEqual(field.index, False)
        self.assertEqual(field.as_string, True)

    def testCreate(self):
        models = self.mapper
        mean = Decimal("56.4")
        started = date(2010, 1, 1)
        timestamp = datetime.now()
        a = yield models.statistics.new(
            dt=date.today(),
            data={
                "mean": mean,
                "std": 5.78,
                "started": started,
                "timestamp": timestamp,
            },
        )
        self.assertEqual(a.data["mean"], mean)
        a = yield models.statistics.get(id=a.id)
        self.assertEqual(len(a.data), 4)
        self.assertEqual(a.data["mean"], mean)
        self.assertEqual(a.data["started"], started)
        self.assertAlmostEqual(
            date2timestamp(a.data["timestamp"]), date2timestamp(timestamp), 5
        )

    def testCreateFromString(self):
        models = self.mapper
        mean = "mean"
        timestamp = time.time()
        data = {"mean": mean, "std": 5.78, "timestamp": timestamp}
        datas = json.dumps(data)
        a = yield models.statistics.new(dt=date.today(), data=datas)
        a = yield models.statistics.get(id=a.id)
        self.assertEqual(a.data["mean"], mean)
        a = yield models.statistics.get(id=a.id)
        self.assertEqual(len(a.data), 3)
        self.assertEqual(a.data["mean"], mean)
        self.assertAlmostEqual(a.data["timestamp"], timestamp)

    def test_default(self):
        models = self.mapper
        a = Statistics(dt=date.today())
        self.assertEqual(a.data, {})
        yield models.add(a)
        self.assertEqual(a.data, {})
        a = yield models.statistics.get(id=a.id)
        self.assertEqual(a.data, {})

    def testValueError(self):
        models = self.mapper
        a = models.statistics(dt=date.today(), data={"mean": self})
        yield self.async.assertRaises(stdnet.FieldValueError, models.session().add, a)
        self.assertTrue("data" in a._dbdata["errors"])

    def testDefaultValue(self):
        models = self.mapper
        role = models.role(name="test")
        self.assertEqual(role.permissions, [])
        role.permissions.append("ciao")
        role.permissions.append(4)
        yield models.session().add(role)
        self.assertTrue(role.id)
        role = yield models.role.get(id=role.id)
        self.assertEqual(role.permissions, ["ciao", 4])


class TestJsonFieldAsData(test.TestCase):
    """Test a model with a JSONField which expand as instance fields.
    The `as_string` atttribute is set to ``False``."""

    model = Statistics3
    def_data = {"mean": 1.0, "std": 5.78, "pv": 3.2, "name": "bla", "dt": date.today()}

    def_baddata = {
        "": 3.2,
        "ts": {"a": [1, 2, 3, 4, 5, 6, 7], "b": [10, 11, 12]},
        "mean": {"1y": 1.0, "2y": 1.1},
        "std": {"1y": 4.0, "2y": 5.1},
        "dt": datetime.now(),
    }

    def_data2 = {
        "pv": {
            "": 3.2,
            "ts": {"a": [1, 2, 3, 4, 5, 6, 7], "b": [10, 11, 12]},
            "mean": {"1y": 1.0, "2y": 1.1},
            "std": {"1y": 4.0, "2y": 5.1},
        },
        "dt": datetime.now(),
    }

    def make(self, data=None, name=None):
        data = data or self.def_data
        name = name or self.data.random_string()
        return self.model(name=name, data=data)

    def testMeta(self):
        field = self.model._meta.dfields["data"]
        self.assertFalse(field.as_string)

    def testMake(self):
        m = self.make()
        self.assertTrue(m.is_valid())
        data = m._dbdata["cleaned_data"]
        data.pop("data")
        self.assertEqual(len(data), 6)
        self.assertEqual(float(data["data__mean"]), 1.0)
        self.assertEqual(float(data["data__std"]), 5.78)
        self.assertEqual(float(data["data__pv"]), 3.2)

    def testGet(self):
        models = self.mapper
        session = models.session()
        m = yield session.add(self.make())
        m = yield models.statistics3.get(id=m.id)
        self.assertEqual(m.data["mean"], 1.0)
        self.assertEqual(m.data["std"], 5.78)
        self.assertEqual(m.data["pv"], 3.2)
        self.assertEqual(m.data["dt"], date.today())
        self.assertEqual(m.data["name"], "bla")

    def testmakeEmptyError(self):
        """Here we test when we have a key which is empty."""
        models = self.mapper
        session = models.session()
        m = self.make(self.def_baddata)
        self.assertFalse(m.is_valid())
        yield self.async.assertRaises(stdnet.FieldValueError, session.add, m)

    def testmakeEmpty(self):
        models = self.mapper
        session = models.session()
        m = self.make(self.def_data2)
        self.assertTrue(m.is_valid())
        cdata = m._dbdata["cleaned_data"]
        self.assertEqual(len(cdata), 10)
        self.assertTrue("data" in cdata)
        self.assertEqual(cdata["data__pv__mean__1y"], "1.0")
        obj = yield session.add(m)
        obj = yield models.statistics3.get(id=obj.id)
        self.assertEqual(obj.data["dt"].date(), date.today())
        self.assertEqual(obj.data__dt.date(), date.today())
        self.assertEqual(obj.data["pv"]["mean"]["1y"], 1.0)
        self.assertEqual(obj.data__pv__mean__1y, 1.0)
        self.assertEqual(obj.data__dt.date(), date.today())

    def testmakeEmpty2(self):
        models = self.mapper
        session = models.session()
        m = self.make({"ts": [1, 2, 3, 4]})
        obj = yield models.add(m)
        obj = yield models.statistics3.get(id=obj.id)
        self.assertEqual(obj.data, {"ts": [1, 2, 3, 4]})

    def __testFuzzySmall(self):
        # TODO: This does not pass in pypy
        models = self.mapper
        session = models.session()
        r = make_random()
        data = dict(r.make(nesting=0))
        m = self.make(data)
        self.assertTrue(m.is_valid())
        cdata = m._dbdata["cleaned_data"]
        cdata.pop("data")
        for k in cdata:
            if k is not "name":
                self.assertTrue(k.startswith("data__"))
        obj = yield session.add(m)
        obj = yield models.statistics3.get(id=obj.id)
        self.assertEqualDict(data, obj.data)

    def __testFuzzyMedium(self):
        # TODO: This does not pass in pypy
        models = self.mapper
        session = models.session()
        r = make_random()
        data = dict(r.make(nesting=1))
        m = self.make(data)
        self.assertTrue(m.is_valid())
        cdata = m._dbdata["cleaned_data"]
        cdata.pop("data")
        for k in cdata:
            if k is not "name":
                self.assertTrue(k.startswith("data__"))
        obj = yield session.add(m)
        # obj = self.model.objects.get(id=obj.id)
        # self.assertEqualDict(data,obj.data)

    def __testFuzzy(self):
        # TODO: This does not pass in pypy
        models = self.mapper
        session = models.session()
        r = make_random()
        data = dict(r.make(nesting=3))
        m = self.make(deepcopy(data))
        self.assertTrue(m.is_valid())
        cdata = m._dbdata["cleaned_data"]
        cdata.pop("data")
        for k in cdata:
            if k is not "name":
                self.assertTrue(k.startswith("data__"))
        obj = yield session.add(m)
        # obj = self.model.objects.get(id=obj.id)
        # self.assertEqualDict(data,obj.data)

    def testEmptyDict(self):
        models = self.mapper
        session = models.session()
        r = yield session.add(self.model(name="bla", data={"bla": "ciao"}))
        self.assertEqual(r.data, {"bla": "ciao"})
        r.data = None
        yield session.add(r)
        r = yield models.statistics3.get(id=r.id)
        self.assertEqual(r.data, {})

    def testFromEmpty(self):
        """Test the change of a data jsonfield from empty to populated."""
        models = self.mapper
        session = models.session()
        r = yield models.statistics3.new(name="bla")
        self.assertEqual(r.data, {})
        r.data = {"bla": "ciao"}
        yield session.add(r)
        r = yield models.statistics3.get(id=r.id)
        self.assertEqual(r.data, {"bla": "ciao"})

    def assertEqualDict(self, data1, data2):
        for k in list(data1):
            v1 = data1.pop(k)
            v2 = data2.pop(k, {})
            if isinstance(v1, dict):
                self.assertEqualDict(v1, v2)
            else:
                self.assertAlmostEqual(v1, v2)
        self.assertFalse(data1)
        self.assertFalse(data2)
