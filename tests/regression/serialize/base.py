from stdnet import orm, test


class SerializerMixin(object):
    serializer = 'json'
    
    def get(self, **options):
        s = orm.get_serializer(self.serializer)
        if not s.default_options:
            self.assertEqual(s.options, options)
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


class DummySerializer(orm.Serializer):
    pass


class TestMeta(test.TestCase):
    
    def testBadSerializer(self):
        self.assertRaises(ValueError, orm.get_serializer, 'djsbvjchvsdjcvsdj')
        
    def testRegisterUnregister(self):
        orm.register_serializer('dummy',DummySerializer())
        s = orm.get_serializer('dummy')
        self.assertTrue('dummy' in orm.all_serializers())
        self.assertTrue(isinstance(s,DummySerializer))
        self.assertRaises(NotImplementedError, s.serialize, None)
        self.assertRaises(NotImplementedError, s.write)
        self.assertRaises(NotImplementedError, s.load, None)
        self.assertTrue(orm.unregister_serializer('dummy'))
        self.assertRaises(ValueError, orm.get_serializer, 'dummy')
        