'''Multivariate numeric timeseries interface.'''
from stdnet import odm, SessionNotAvailable, InvalidTransaction
from stdnet.utils.skiplist import skiplist
from stdnet.utils.async import on_result, async
from stdnet.utils import encoders, iteritems, zip, iterpair


__all__ = ['TimeseriesCache', 'ColumnTS', 'ColumnTSField', 'as_dict']


class TimeseriesCache(object):
    cache = None
    def __init__(self):
        self.merged_series = None
        self.fields = {}
        self.delete_fields = set()
        self.deleted_timestamps = set()

    def add(self, timestamp, field, value):
        if field not in self.fields:
            self.fields[field] = skiplist()
        self.fields[field].insert(timestamp,value)

    def clear(self):
        self.merged_series = None
        self.fields.clear()
        self.delete_fields.clear()
        self.deleted_timestamps.clear()


def as_dict(times, fields):
    lists = []
    names = []
    d = {}
    for name, value in fields.items():
        names.append(name)
        lists.append(value)
    for dt, data in zip(times, zip(*lists)):
        d[dt] = dict(zip(names,data))
    return d
        

class ColumnTS(odm.TS):
    '''A specialised :class:`stdnet.odm.TS` structure for numeric
multivariate timeseries.'''
    default_multi_stats = ['covariance']

    cache_class = TimeseriesCache
    pickler = encoders.DateTimeConverter()
    value_pickler = encoders.Double()

    def front(self, *fields):
        '''Return the front pair of the structure'''
        v,f = tuple(self.irange(0, 0, fields=fields))
        if v:
            return (v[0],dict(((field, f[field][0]) for field in f)))

    def back(self, *fields):
        '''Return the back pair of the structure'''
        v,f = tuple(self.irange(-1, -1, fields=fields))
        if v:
            return (v[0],dict(((field, f[field][0]) for field in f)))

    def info(self, start=None, end=None, fields=None):
        '''Provide data information for this :class:`ColumnTS`. If no
parameters are specified it returns the number of data points for each
fields, as well as the start and end date.'''
        start = self.pickler.dumps(start) if start else None
        end = self.pickler.dumps(end) if end else None
        return on_result(self.backend_structure().info(start, end, fields),
                         self._stats)

    def fields(self):
        '''Tuple of ordered fields for this :class:`ColumnTS`.'''
        return self.backend_structure().fields()

    def numfields(self):
        '''Number of fields.'''
        return self.backend_structure().numfields()

    @odm.commit_when_no_transaction
    def add(self, dt, *args):
        self._add(dt, *args)
        return self

    @odm.commit_when_no_transaction
    def update(self, mapping):
        if isinstance(mapping, dict):
            mapping = iteritems(mapping)
        add = self._add
        for dt, v in mapping:
            add(dt, v)
        return self

    def evaluate(self, script, *series, **params):
        res = self.backend_structure().run_script('evaluate', series,
                                                  script, **params)
        return on_result(res, self._evaluate)

    def istats(self, start=0, end=-1, fields=None):
        '''Perform a multivariate statistic calculation of this
:class:`ColumnTS` from *start* to *end*.

:param start: Optional index (rank) where to start the analysis.
:param end: Optional index (rank) where to end the analysis.
:param fields: Optional subset of :meth:`fields` to perform analysis on.
    If not provided all fields are included in the analysis.
'''
        res = self.backend_structure().istats(start, end, fields)
        return on_result(res, self._stats)

    def stats(self, start, end, fields=None):
        '''Perform a multivariate statistic calculation of this
:class:`ColumnTS` from a *start*  date/datetime to an 
*end* date/datetime.

:param start: Start date for analysis.
:param end: End date for analysis.
:param fields: Optional subset of :meth:`fields` to perform analysis on.
    If not provided all fields are included in the analysis.
'''
        start = self.pickler.dumps(start)
        end = self.pickler.dumps(end)
        res = self.backend_structure().stats(start, end, fields)
        return on_result(res, self._stats)

    def imulti_stats(self, start=0, end=-1, series=None, fields=None,
                     stats=None):
        '''Perform cross multivariate statistics calculation of
this :class:`ColumnTS` and other optional *series* from *start*
to *end*.

:parameter start: the start rank.
:parameter start: the end rank
:parameter field: name of field to perform multivariate statistics.
:parameter series: a list of two elements tuple containing the id of the
    a :class:`columnTS` and a field name.
:parameter stats: list of statistics to evaluate.
    Default: ['covariance']
'''
        stats = stats or self.default_multi_stats
        res = self.backend_structure().imulti_stats(start, end, fields, series,
                                                    stats)
        return on_result(res, self._stats)

    def multi_stats(self, start, end,  series=None, fields=None, stats=None):
        '''Perform cross multivariate statistics calculation of
this :class:`ColumnTS` and other *series*.

:parameter start: the start date.
:parameter start: the end date
:parameter field: name of field to perform multivariate statistics.
:parameter series: a list of two elements tuple containing the id of the
    a :class:`columnTS` and a field name.
:parameter stats: list of statistics to evaluate.
    Default: ['covariance']
'''
        stats = stats or self.default_multi_stats
        start = self.pickler.dumps(start)
        end = self.pickler.dumps(end)
        res = self.backend_structure().multi_stats(
                        start, end, fields, series, stats)
        return on_result(res, self._stats)

    def merge(self, *series, **kwargs):
        '''Merge this :class:`ColumnTS` with several other *series*.

:parameters series: a list of tuples where the nth element is a tuple
    of the form::

    (wight_n, ts_n1, ts_n2, ..., ts_nMn)

The result will be calculated using the formula::

    ts = weight_1*ts_11*ts_12*...*ts_1M1 + weight_2*ts_21*ts_22*...*ts_2M2 +
         ...
'''
        session = self.session
        if not session:
            raise SessionNotAvailable('No session available')
        self.check_router(session.router, *series)
        return self._merge(*series, **kwargs)

    @classmethod
    def merged_series(cls, *series, **kwargs):
        '''Merge ``series`` and return the results without storing data
in the backend server.'''
        router, backend = cls.check_router(None, *series)
        if backend:
            target = router.register(cls(), backend)
            router.session().add(target)
            target._merge(*series, **kwargs)
            res = target.backend_structure().irange_and_delete()
            return on_result(res, target.load_data)

    # INTERNALS
    @classmethod
    def check_router(cls, router, *series):
        backend = None
        for serie in series:
            if len(serie) < 2:
                raise ValueError('merge requires tuples of length 2 or more')
            for s in serie[1:]:
                if not s.session:
                    raise SessionNotAvailable('No session available')
                if router is None:
                    router = s.session.router
                else:
                    if router is not s.session.router:
                        raise InvalidTransaction('mistmaching routers')
                if backend is None:
                    backend = s.backend_structure().backend
                else:
                    if backend is not s.backend_structure().backend:
                        raise InvalidTransaction('merging is possible only on '
                                                 'the same backend')
        return router, backend

    def _merge(self, *series, **kwargs):
        fields = kwargs.get('fields') or ()
        self.backend_structure().merge(series, fields)
        
    def load_data(self, result):
        #Overwrite :meth:`stdnet.odm.PairMixin.load_data` method
        loads = self.pickler.loads
        vloads = self.value_pickler.loads
        dt = [loads(t) for t in result[0]]
        vals = {}
        for f, data in iterpair(result[1]):
            vals[f] = [vloads(d) for d in data]
        return (dt, vals)

    def load_get_data(self, result):
        vloads = self.value_pickler.loads
        return dict(((f, vloads(v)) for f, v in iterpair(result)))

    def _stats(self, result):
        if result and 'start' in result:
            result['start'] = self.pickler.loads(result['start'])
            result['stop'] = self.pickler.loads(result['stop'])
        return result

    def _evaluate(self, result):
        return result

    def _add(self, dt, *args):
        timestamp = self.pickler.dumps(dt)
        add = self.cache.add
        dump = self.value_pickler.dumps
        if len(args) == 1:
            mapping = args[0]
            if isinstance(mapping, dict):
                mapping = iteritems(mapping)
            for field, value in mapping:
                add(timestamp, field, dump(value))
        elif len(args) == 2:
            add(timestamp, args[0], dump(args[1]))
        else:
            raise TypeError('Expected a mapping or a field value pair')


class ColumnTSField(odm.StructureField):
    '''A multivariate timeseries field.'''
    type = 'columnts'
    def structure_class(self):
        return ColumnTS

