import stdnet
from stdnet.utils.async import multi_async, async, BackendQuery

from .client import CouchDb, CouchDbError, CouchDbNoDbError


class CouchDbQuery(BackendQuery):
    '''Backend query implementation for CouchDB.'''
    
    def _build(self):
        queryelem = self.queryelem
        keyword = queryelem.keyword
        pkname = queryelem.meta.pkname()
        data = {}
        if keyword == 'union':
            expr = ' && '.join(self._logical(queryelem))
        elif keyword == 'intersection':
            expr = ' || '.join(self._logical(queryelem))
        elif keyword == 'diff':
            expr = ' && !'.join(self._logical(queryelem))
        else:
            expr = self._accumulate(self._selectors(queryelem))
        #
        where = self.queryelem.data.get('where')
        if where:
            expr = '(%s) && (%s)' % (expr, where[0])
            
    def _logical(self, queryelem):
        for child in queryelem:
            yield self._accumulate(self._selectors(child))
    
    def _selectors(self, queryelem):
        pkname = queryelem.meta.pkname()
        name = queryelem.name
        for child in queryelem:
            if getattr(child, 'backend', None) is not None:
                lookup, value = 'set', child
            else:
                lookup, value = child
            if name == pkname:
                name = '_id'
            if lookup == 'set':
                if value.meta != queryelem.meta:
                    qs = self.__class__(value)
                    yield name, selector, ids_from_query(qs)
                else:
                    if name == '_id' and not value.underlying:
                        continue
                    else:
                        for n, sel, value in self._selectors(value, selector):
                            yield n, sel, value
            else:
                if lookup == 'value':
                    sel = selector
                else:
                    sel = self.selector_map[lookup]
                yield name, sel, value
            selector = selector2 or selector
    
    def _accumulate(self, data):
        kwargs = {}
        for name, selector, value in data:
            if name in kwargs:
                data = kwargs[name]
                if selector in ('$in', '$nin'):
                    if selector in data:
                        data[selector].append(value)
                    else:
                        data[selector] = [value]
                else:
                    data[selector] = value
            else:
                if selector in ('$in', '$nin') and not isinstance(value, list):
                    value = [value]
                kwargs[name] = {selector: value}
        return kwargs


class BackendDataServer(stdnet.BackendDataServer):
    Query = CouchDbQuery
    default_port = 5984
        
    def setup_connection(self, address):
        if len(address) == 1:
            address.append(self.default_port)
        address = 'http://%s:%s' % tuple(address)
        return CouchDb(address, **self.params)
    
    @async()
    def flush(self, meta=None):
        pattern = self.basekey(meta) if meta else self.namespace
        databases = yield self.client.databases()
        todelete = []
        for database in databases:
            if database.startswith(pattern):
                todelete.append(self.client.deletedb(database))
        yield multi_async(todelete)
        