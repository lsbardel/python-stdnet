from stdnet import test
from stdnet.utils import zip

from examples.models import SimpleModel


class testLoadOnly(test.TestCase):
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
        self.assertEqual(self.model.objects.filter(group = 'group1').count(),3)
        qs = self.model.objects.all().load_only('code')
        for m in qs:
            m.save()
        qs = self.model.objects.all()
        for m,g in zip(qs,original):
            self.assertEqual(m.group,g)
        # No check indexes
        self.assertEqual(self.model.objects.filter(group = 'group1').count(),3)
        
    def testChangeNotLoaded(self):
        '''We load an object with only one field nad modify a field not
loaded. The correct behavior should be to updated the field and indexes.'''
        original = [m.group for m in self.model.objects.all()]
        qs = self.model.objects.all().load_only('code')
        for m in qs:
            m.group = 'group4'
            m.save()
        qs = self.model.objects.filter(group = 'group1')
        self.assertEqual(qs.count(),0)
        qs = self.model.objects.filter(group = 'group2')
        self.assertEqual(qs.count(),0)
        qs = self.model.objects.filter(group = 'group3')
        self.assertEqual(qs.count(),0)
        qs = self.model.objects.filter(group = 'group4')
        self.assertEqual(qs.count(),5)
        for m in qs:
            self.assertEqual(m.group,'group4')
        