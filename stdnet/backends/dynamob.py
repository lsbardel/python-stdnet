import os

try:
    import boto
    from boto.exception import BotoServerError
except ImportError:     #pragma: no cover
    raise RuntimeError("You need boto installed to use Dynamo backend.")

import stdnet 

################################################################################
##    DYNAMO BACKEND
################################################################################
class BackendDataServer(stdnet.BackendDataServer):
    '''Amazon DynamoDB organizes data into tables containing items,
and each item has one or more attributes.'''
    def setup_connection(self, address):
        ak = self.params.pop('ak', None)
        sk = self.params.pop('sk', None)
        proxy = self.params.pop('proxy', None)
        if proxy:
            os.environ['http_proxy'] = proxy
        try:
            if sk and ak:
                ak = ak.replace(' ','+')
                sk = sk.replace(' ','+')
            return boto.connect_dynamodb(
                            aws_access_key_id=ak,
                            aws_secret_access_key=sk,
                            host=address, **self.params)
        except BotoServerError as e:
            msg = e.error_message or str(e)
            raise stdnet.ConnectionError(msg)
        
    def flush(self, meta=None, pattern=None):
        pass
    
    def execute_session(self, session, callback):
        '''Execute a session in dynamo.'''
        for sm in session:
            meta = sm.meta
            table = self.get_or_create_table(meta)
            
    #    INTERNALS
    
    def get_or_create_table(self, meta):
        table_name = self.basekey(meta)
        client = self.client
        client.list_tables()
        table = client.get_table(table_name)
        pk = meta.pk
        if pk.type == 'composite':
            pass
        else:
            proto = 'N' if pk.internal_type=='numeric' else 'S'
            schema = self.client.create_schema(hash_key_name=pk.name,
                                               hash_key_proto_value=proto)
        return self.client.create_table(name=table_name,
                                        schema=schema)
    