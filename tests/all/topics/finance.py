import datetime
import logging
from random import randint

from stdnet import QuerySetError
from stdnet.utils import test

from examples.models import Instrument, Fund, Position, PortfolioView,\
                             UserDefaultView
from examples.data import finance_data, INSTS_TYPES, CCYS_TYPES


class TestFinanceApplication(test.TestWrite):
    data_cls = finance_data
    models = (Instrument, Fund, Position)

    def testGetObject(self):
        '''Test get method for id and unique field'''
        session = yield self.data.create(self)
        query = session.query(Instrument)
        obj = yield query.get(id=2)
        self.assertEqual(obj.id, 2)
        self.assertTrue(obj.name)
        obj2 = yield query.get(name=obj.name)
        self.assertEqual(obj, obj2)

    def testLen(self):
        '''Simply test len of objects greater than zero'''
        session = yield self.data.create(self)
        objs = yield session.query(Instrument).all()
        self.assertTrue(len(objs) > 0)

    def testFilter(self):
        '''Test filtering on a model without foreign keys'''
        yield self.data.create(self)
        session = self.session()
        query = session.query(Instrument)
        self.async.assertRaises(QuerySetError, query.get, type='equity')
        tot = 0
        for t in INSTS_TYPES:
            fs = query.filter(type=t)
            all = yield fs.all()
            count = {}
            for f in all:
                count[f.ccy] = count.get(f.ccy, 0) + 1
            for c in CCYS_TYPES:
                x = count.get(c,0)
                objs = yield fs.filter(ccy=c).all()
                y = 0
                for obj in objs:
                    y += 1
                    tot += 1
                    self.assertEqual(obj.type, t)
                    self.assertEqual(obj.ccy, c)
                self.assertEqual(x,y)
        all = query.all()
        self.assertEqual(tot, len(all))

    def testValidation(self):
        pos = Position(size=10)
        self.assertFalse(pos.is_valid())
        self.assertEqual(len(pos._dbdata['errors']),3)
        self.assertEqual(len(pos._dbdata['cleaned_data']),1)
        self.assertTrue('size' in pos._dbdata['cleaned_data'])

    def testForeignKey(self):
        '''Test filtering with foreignkeys'''
        session = yield self.data.makePositions(self)
        query = session.query(Position).load_related('instrument').load_related('fund')
        #
        positions = yield query.all()
        self.assertTrue(positions)
        #
        multi = []
        for p in positions:
            self.assertTrue(isinstance(p.instrument, Instrument))
            self.assertTrue(isinstance(p.fund, Fund))
            multi.append(query.filter(instrument=p.instrument, fund=p.fund).all())
        multi = yield self.multi_async(multi)
        for p, pos in zip(positions, multi):
            self.assertTrue(p in pos)
        #
        # Testing
        total_positions = len(positions)
        totp = 0
        multi = []
        instruments = yield session.query(Instrument).all()
        #
        for instrument in instruments:
            multi.append(instrument.positions.query().load_related('instrument').all())
        multi = yield self.multi_async(multi)
        #
        for instrument, pos in zip(instruments, multi):
            for p in pos:
                self.assertTrue(isinstance(p, Position))
                self.assertEqual(p.instrument, instrument)
            totp += len(pos)
        #
        self.assertEqual(total_positions, totp)

    def testRelatedManagerFilter(self):
        session = yield self.data.makePositions(self)
        instruments = session.query(Instrument)
        for instrument in instruments:
            positions = instrument.positions.query()
            funds = {}
            flist = []
            for pos in positions:
                fund = pos.fund
                n    = funds.get(fund.id,0) + 1
                funds[fund.id] = n
                if n == 1:
                    flist.append(fund)
            for fund in flist:
                positions = instrument.positions.filter(fund = fund)
                self.assertEqual(len(positions),funds[fund.id])

    def testDeleteSimple(self):
        '''Test delete on models without related models'''
        session = yield self.data.create(self)
        instruments = session.query(Instrument)
        funds = session.query(Fund)
        self.assertTrue(instruments.count())
        self.assertTrue(funds.count())
        instruments.delete()
        funds.delete()
        self.assertFalse(session.query(Instrument).count())
        self.assertFalse(session.query(Fund).count())

    def testDelete(self):
        '''Test delete on models with related models'''
        # Create Positions which hold foreign keys to Instruments
        session = yield self.data.makePositions(self)
        instruments = session.query(Instrument)
        positions = session.query(Position)
        self.assertTrue(instruments.count())
        self.assertTrue(positions.count())
        instruments.delete()
        self.assertFalse(session.query(Instrument).count())
        self.assertFalse(session.query(Position).count())

