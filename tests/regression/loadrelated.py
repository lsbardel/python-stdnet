from examples.data import FinanceTest, Position, Instrument, Fund


class load_related(FinanceTest):
    
    def testSelectRelated(self):
        self.data.makePositions(self)
        pos = Position.objects.all().load_related()
        self.assertTrue(pos._select_related)
        self.assertTrue(len(pos._select_related),2)
        for p in pos:
            for f in pos._select_related:
                cache = f.get_cache_name()
                val = getattr(p,cache,None)
                self.assertTrue(val)
                self.assertTrue(isinstance(val,f.relmodel))
                id = getattr(p,f.attname)
                self.assertEqual(id,val.id)
        
    def testSelectRelatedSingle(self):
        self.data.makePositions(self)
        pos = Position.objects.all().load_related('instrument')
        self.assertTrue(pos._select_related)
        self.assertTrue(len(pos._select_related),1)
        fund = Position._meta.dfields['fund']
        inst = Position._meta.dfields['instrument']
        pos = list(pos)
        self.assertTrue(pos)
        for p in pos:
            cache = inst.get_cache_name()
            val = getattr(p,cache,None)
            self.assertTrue(val)
            self.assertTrue(isinstance(val,inst.relmodel))
            cache = fund.get_cache_name()
            val = getattr(p,cache,None)
            self.assertFalse(val)
            
