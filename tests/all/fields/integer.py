from stdnet import FieldValueError
from stdnet.utils import test

from examples.models import Page


class TestIntegerField(test.TestCase):
    model = Page

    def test_default_value(self):
        models = self.mapper
        p = Page()
        self.assertEqual(p.in_navigation, 1)
        p = Page(in_navigation='4')
        self.assertEqual(p.in_navigation, 4)
        self.assertRaises(FieldValueError, p=Page, in_navigation='foo')
        yield self.session().add(p)
        self.assertEqual(p.in_navigation, 4)
        p = yield models.page.get(id=p.id)
        self.assertEqual(p.in_navigation, 4)

    def testNotValidated(self):
        models = self.mapper
        p = yield models.page.new()
        self.assertEqual(p.in_navigation, 1)
        self.assertRaises(ValueError, Page, in_navigation='bla')

    def testZeroValue(self):
        models = self.mapper
        p = models.page(in_navigation=0)
        self.assertEqual(p.in_navigation, 0)
        yield models.session().add(p)
        self.assertEqual(p.in_navigation, 0)
        p = yield models.page.get(id=p.id)
        self.assertEqual(p.in_navigation, 0)
