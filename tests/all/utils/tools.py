import time
from datetime import date, datetime

from examples.models import Statistics3

import stdnet
from stdnet import odm
from stdnet.utils import (
    _format_int,
    addmul_number_dicts,
    date2timestamp,
    encoders,
    grouper,
    populate,
    test,
    timestamp2date,
    to_bytes,
    to_string,
)
from stdnet.utils.version import get_git_changeset


class TestUtils(test.TestCase):
    multipledb = False
    model = Statistics3

    def __testNestedJasonValue(self):
        data = {"data": 1000, "folder1": {"folder11": 1, "folder12": 2, "": "home"}}
        session = self.session()
        with session.begin():
            session.add(self.model(name="foo", data=data))
        obj = session.query(self.model).get(id=1)
        self.assertEqual(
            nested_json_value(obj, "data__folder1__folder11", odm.JSPLITTER), 1
        )
        self.assertEqual(
            nested_json_value(obj, "data__folder1__folder12", odm.JSPLITTER), 2
        )
        self.assertEqual(nested_json_value(obj, "data__folder1", odm.JSPLITTER), "home")

    def test_date2timestamp(self):
        t1 = datetime.now()
        ts1 = date2timestamp(t1)
        self.assertAlmostEqual(ts1, date2timestamp(t1))
        t1 = date.today()
        ts1 = date2timestamp(t1)
        t = timestamp2date(ts1)
        self.assertEqual(t.date(), t1)
        self.assertEqual(t.hour, 0)
        self.assertEqual(t.minute, 0)
        self.assertEqual(t.second, 0)
        self.assertEqual(t.microsecond, 0)

    def test_addmul_number_dicts(self):
        d1 = {"bla": 2.5, "foo": 1.1}
        d2 = {"bla": -2, "foo": -0.3}
        r = addmul_number_dicts(((2, d1), (-1, d2)))
        self.assertEqual(len(r), 2)
        self.assertAlmostEqual(r["bla"], 7)
        self.assertAlmostEqual(r["foo"], 2.5)

    def test_addmul_number_dicts2(self):
        d1 = {"bla": 2.5, "foo": 1.1}
        d2 = {"bla": -2, "foo": -0.3, "moon": 8.5}
        r = addmul_number_dicts(((2, d1), (-1, d2)))
        self.assertEqual(len(r), 2)
        self.assertEqual(r["bla"], 7)
        self.assertEqual(r["foo"], 2.5)

    def test_addmul_number_dicts3(self):
        series = [
            (
                1.0,
                {
                    "carry1w": 0.08903324115987132,
                    "pv": "17.7",
                    "carry3m": 1.02,
                    "carry6m": 1.9645094151419826,
                    "carry1y": 3.7291316215073422,
                    "irdelta": "#Err",
                },
            ),
            (
                1.0,
                {
                    "carry1w": 0.025649796255470036,
                    "pv": 12.1,
                    "carry3m": "-0.61",
                    "carry6m": 1.77763873433023,
                    "carry1y": 5.566080890214712,
                    "irdelta": "#Err",
                },
            ),
            (-1.0, {"carry1w": "#Err", "pv": 18.1, "carry3m": -0.04, "irdelta": 1}),
        ]
        r = addmul_number_dicts(series)
        self.assertEqual(len(r), 2)
        self.assertAlmostEqual(r["pv"], 11.7)
        self.assertAlmostEqual(r["carry3m"], 0.45)

    def test_addmul_nested_dicts(self):
        d1 = {"bla": {"bla1": 2.5}, "foo": 1.1}
        d2 = {"bla": {"bla1": -2}, "foo": -0.3, "moon": 8.5}
        r = addmul_number_dicts(((2, d1), (-1, d2)))
        self.assertEqual(len(r), 2)
        self.assertEqual(r["bla"]["bla1"], 7)
        self.assertEqual(r["foo"], 2.5)


class testFunctions(test.TestCase):
    def testGrouper(self):
        r = grouper(2, [1, 2, 3, 4, 5, 6, 7])
        self.assertFalse(hasattr(r, "__len__"))
        self.assertEqual(list(r), [(1, 2), (3, 4), (5, 6), (7, None)])
        r = grouper(3, "abcdefg", "x")
        self.assertFalse(hasattr(r, "__len__"))
        self.assertEqual(list(r), [("a", "b", "c"), ("d", "e", "f"), ("g", "x", "x")])

    def testFormatInt(self):
        self.assertEqual(_format_int(4500), "4,500")
        self.assertEqual(_format_int(4500780), "4,500,780")
        self.assertEqual(_format_int(500), "500")
        self.assertEqual(_format_int(-780), "-780")
        self.assertEqual(_format_int(-4500780), "-4,500,780")

    def testPopulateIntegers(self):
        data = populate("integer", size=33)
        self.assertEqual(len(data), 33)
        for d in data:
            self.assertTrue(isinstance(d, int))

    def testAbstarctEncoder(self):
        e = encoders.Encoder()
        self.assertRaises(NotImplementedError, e.dumps, "bla")
        self.assertRaises(NotImplementedError, e.loads, "bla")

    def test_to_bytes(self):
        self.assertEqual(to_bytes(b"ciao"), b"ciao")
        b = b"perch\xc3\xa9"
        u = b.decode("utf-8")
        l = u.encode("latin")
        self.assertEqual(to_bytes(b, "latin"), l)
        self.assertEqual(to_string(l, "latin"), u)
        self.assertEqual(to_bytes(1), b"1")

    def test_git_version(self):
        g = get_git_changeset()
        # In travis this is None.
        # TODO: better test on this
        # self.assertTrue(g)
