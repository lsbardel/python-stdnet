from stdnet import test
from stdnet.utils import zip

from examples.models import SimpleModel


class testLoadOnly(test.TestModelBase):
    model = SimpleModel
    
    def setUp(self):
        self.model(code = 'a', group = 'group1', description = 'blabla').save()
        self.model(code = 'b', group = 'group2', description = 'blabla').save()
        self.model(code = 'c', group = 'group1', description = 'blabla').save()
        self.model(code = 'd', group = 'group3', description = 'blabla').save()
        self.model(code = 'e', group = 'group1', description = 'blabla').save()
        
    def testSimple(self):
        qs = self.model.objects.all().load_only('code')
        for m in qs:
            self.assertEqual(m._loadedfields,('code',))
            self.assertTrue(m.code)
            self.assertFalse(m.group)
            self.assertFalse(m.description)
        qs = self.model.objects.all().load_only('code','group')
        for m in qs:
            self.assertEqual(m._loadedfields,('code','group'))
            self.assertTrue(m.code)
            self.assertTrue(m.group)
            self.assertFalse(m.description)
            
    def testSave(self):
        original = [m.group for m in self.model.objects.all()]
        qs = self.model.objects.all().load_only('code')
        for m in qs:
            m.save()
        qs = self.model.objects.all()
        for m,g in zip(qs,original):
            self.assertEqual(m.group,g)
        