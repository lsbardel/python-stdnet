'''Amazon Web Services Databases

It requires boto_

.. _boto: https://github.com/boto/boto
'''
import time
from copy import copy
try:
    import boto
except ImportError:
    boto = None
    
if boto is not None:
    from boto.exception import DynamoDBResponseError
    
import stdnet
from stdnet.utils import grouper


class DynamoClient(object):
    
    def __init__(self, access_key = None, secret_key = None, **params):
        self.params = params
        self.keys = (access_key, secret_key)
        self._tables = {}
        self._connection = boto.connect_dynamodb(*self.keys)
        
    def clone(self):
        d = copy(self)
        d._tables = {}
        d._connection = boto.connect_dynamodb(*d.keys)
        return d
        
    def delete_table(self, table):
        if hasattr(table,'name'):
            table_name = table.name
        else:
            table_name = table
        self._tables.pop(table_name,None)
        self._connection.delete_table(table)
        
    def table(self, name, meta = None):
        if name not in self._tables:
            try:
                table = self._connection.get_table(name)
            except DynamoDBResponseError:
                if meta is not None:
                    table = self.create_table(name, meta)
                else:
                    raise
            self._tables[name] = table
        return self._tables[name]
        
    def tables(self, prefix = None):
        tables = self._connection.list_tables()
        if prefix:
            tables = list((t for t in tables if t.startswith(prefix)))
        return tables
        
    def create_table(self, name, meta):
        '''Create a new table'''
        table_schema = self._connection.create_schema(
                hash_key_name=meta.pkname(),
                hash_key_proto_value=meta.pk.python_type)
        return self._connection.create_table(
                name=name,
                schema=table_schema,
                read_units=10,
                write_units=10)


class BackendDataServer(stdnet.BackendDataServer):
    
    def setup_connection(self, address, ak = None, sk = None, **params):
        import boto
        if boto is None:
            raise ImportError('Dynamo Backend requires boto')
        return DynamoClient(ak, sk, **params)
    
    def flush(self, meta = None, pattern = None):
        if meta is None:
            pattern = pattern if pattern is not None else self.namespace
            if pattern.endswith('*'):
                pattern = pattern[:-1]
        else:
            pattern = self.format(self.basekey(meta))
        for table in self.client.tables(pattern):
            try:
                self.client.delete_table(table)
            except DynamoDBResponseError as e:
                # wait 2 seconds
                time.sleep(2)
            
    def execute_session(self, session, callback):
        '''Execute a session in dynamo.'''
        basekey = self.basekey
        client = self.client
        result = []
        for sm in session:
            meta = sm.meta
            model_type = meta.model._model_type
            if model_type == 'structure':
                self.flush_structure(sm, pipe)
            elif model_type == 'object':
                #delquery = sm.get_delete_query(pipe = pipe)
                #self.accumulate_delete(pipe, delquery)
                dirty = tuple(sm.iterdirty())
                N = len(dirty)
                if N:
                    items = []
                    bk = basekey(meta)
                    table = client.table(bk, meta)
                    for instance in dirty:
                        state = instance.state()
                        if not instance.is_valid():
                            raise FieldValueError(
                                        json.dumps(instance._dbdata['errors']))
                        data = instance._dbdata['cleaned_data']
                        item = table.new_item(state.iid, attrs=data)
                        if state.persistent:
                            item.save()
                        else:
                            item.put()
                        items.append(instance)
                    result.append((meta,items))
        return callback(result, None)
        
        