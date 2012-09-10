'''Experimental!
This is an experimental module for converting ColumnTS into
dynts.timeseries. It requires dynts_.

.. _dynts: https://github.com/quantmind/dynts
'''
from collections import Mapping

from . import models as columnts

import numpy as ny

from dynts import timeseries, tsname


class ColumnTS(columnts.ColumnTS):
    '''Integrate stdnet timeseries with dynts_ TimeSeries'''

    def front(self, *fields):
        '''Return the front pair of the structure'''
        ts = self.irange(0, 0, fields = fields)
        if ts:
            return ts.start(),ts[0]

    def back(self, *fields):
        '''Return the back pair of the structure'''
        ts = self.irange(-1, -1, fields = fields)
        if ts:
            return ts.end(),ts[0]

    def load_data(self, result):
        loads = self.pickler.loads
        vloads = self.value_pickler.loads
        dt, va = result
        if result[0] and va:
            dates = ny.array([loads(t) for t in dt])
            fields = []
            vals = []
            if not isinstance(va, Mapping):
                va = dict(va)
            for f in sorted(va):
                fields.append(f)
                data = va[f]
                vals.append((vloads(v) for v in data))
            values = ny.array(list(zip(*vals)))
            name = tsname(*fields)
        else:
            name = None
            dates = None
            values = None
        return timeseries(name=name, date=dates, data=values)

    def _get(self, result):
        ts = self.load_data(result)
        return ts[0]


class ColumnTSField(columnts.ColumnTSField):

    def structure_class(self):
        return ColumnTS
