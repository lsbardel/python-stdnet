"""tests for odm.StringField"""
from examples.models import SimpleString

from stdnet.utils import iteritems, populate, test, to_string, zip

from .struct import MultiFieldMixin


class TestStringField(MultiFieldMixin, test.TestCase):
    multipledb = "redis"
    model = SimpleString

    def adddata(self, li):
        """Add elements to a list without using transactions."""
        for elem in self.data.names:
            li.data.push_back(elem)
        yield self.async.assertEqual(li.data.size(), len("".join(self.data.names)))

    def test_incr(self):
        m = yield self.session().add(self.model())
        self.async.assertEqual(m.data.incr(), 1)
        self.async.assertEqual(m.data.incr(), 2)
        self.async.assertEqual(m.data.incr(3), 5)
        self.async.assertEqual(m.data.incr(-7), -2)
