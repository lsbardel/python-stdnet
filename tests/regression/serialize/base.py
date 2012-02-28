__test__ = False

from stdnet import orm


class SerializerMixin(object):
    serializer = 'json'
    
    def get(self, **options):
        s = orm.get_serializer(self.serializer)
        self.assertEqual(s.options,options)
        self.assertFalse(s.data)
        self.assertTrue(s)
        return s
        
    def testMeta(self):
        self.get()
        
    def testDump(self):
        self.data.create(self)
        s = self.get()
        qs = self.model.objects.query().sort_by('id')
        s.serialize(qs)
        self.assertTrue(s.data)
        return s
    
    def testWrite(self):
        s = self.testDump()
        data = s.write()
        self.assertTrue(data)
        
    def testLoad(self):
        s = self.testDump()
        data = s.write().getvalue()
        self.model.objects.flush()
        s.load(data)
        