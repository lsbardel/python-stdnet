from stdnet.utils import Interval, Intervals, pickle, test


class TestInterval(test.TestCase):
    def intervals(self):
        a = Interval(4, 6)
        b = Interval(8, 10)
        intervals = Intervals((b, a))
        self.assertEqual(len(intervals), 2)
        self.assertEqual(intervals[0], a)
        self.assertEqual(intervals[1], b)
        return intervals

    def testSimple(self):
        a = Interval(4, 6)
        self.assertEqual(a.start, 4)
        self.assertEqual(a.end, 6)
        self.assertEqual(tuple(a), (4, 6))
        self.assertRaises(ValueError, Interval, 6, 3)

    def testPickle(self):
        a = Interval(4, 6)
        s = pickle.dumps(a)
        b = pickle.loads(s)
        self.assertEqual(type(b), tuple)
        self.assertEqual(len(b), 2)
        self.assertEqual(b[0], 4)
        self.assertEqual(b[1], 6)

    def testPickleIntervals(self):
        a = self.intervals()
        s = pickle.dumps(a)
        b = pickle.loads(s)
        self.assertEqual(type(b), list)
        self.assertEqual(len(b), len(a))

    def testmultiple(self):
        i = self.intervals()
        a = Interval(20, 30)
        i.append(a)
        self.assertEqual(len(i), 3)
        self.assertEqual(i[-1], a)
        i.append(Interval(18, 21))
        self.assertEqual(len(i), 3)
        self.assertNotEqual(i[-1], a)
        self.assertEqual(i[-1].start, 18)
        self.assertEqual(i[-1].end, 30)
        i.append(Interval(8, 10))
        self.assertEqual(len(i), 3)
        self.assertEqual(i[-2].start, 8)
        self.assertEqual(i[-2].end, 10)
        i.append(Interval(8, 25))
        self.assertEqual(len(i), 2)
        self.assertEqual(i[-1].start, 8)
        self.assertEqual(i[-1].end, 30)
        i.append(Interval(1, 40))
        self.assertEqual(len(i), 1)
        self.assertEqual(i[0].start, 1)
        self.assertEqual(i[0].end, 40)

    def testAppendtuple(self):
        i = self.intervals()
        i.append((18, 21))
        self.assertEqual(len(i), 3)
        self.assertEqual(i[-1].start, 18)
        self.assertEqual(i[-1].end, 21)
        self.assertRaises(TypeError, i.append, 3)
        self.assertRaises(ValueError, i.append, (8, 2))
