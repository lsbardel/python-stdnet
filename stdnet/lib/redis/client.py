import redis
from redis.client import pairs_to_dict, BasePipeline

from .scripts import get_script
from .prefixed import *

__all__ = ['pairs_to_dict', 'Redis', 'ConnectionPool']

RedisError = redis.RedisError
ConnectionError = redis.ConnectionError

#redis_before_send = Signal()
#redis_after_receive = Signal()


class ConnectionPool(redis.ConnectionPool):
    '''Synchronous Redis connection pool compatible with the Asynchronous One'''
    def __init__(self, address, **kwargs):
        if isinstance(address, tuple):
            host, port = address
            kwargs['host'] = host
            kwargs['port'] = port
        else:
            kwargs['path'] = address
        super(ConnectionPool, self).__init__(**kwargs)
        self.loaded_scripts = set()
        
    def request(self, client, *args, **options):
        command_name = args[0]
        connection = self.get_connection(command_name, **options)
        try:
            connection.send_command(*args)
            return client.parse_response(connection, command_name, **options)
        except ConnectionError:
            connection.disconnect()
            connection.send_command(*args)
            return client.parse_response(connection, command_name, **options)
        finally:
            self.release(connection)
            
    def request_pipeline(self, client, raise_on_error=True):
        return client.super_execute(raise_on_error)
        

class Redis(redis.StrictRedis):
    
    def execute_command(self, *args, **options):
        "Execute a command and return a parsed response"
        return self.connection_pool.request(self, *args, **options)
    
    def pipeline(self, transaction=True, shard_hint=None):
        return Pipeline(self, transaction, shard_hint)

    ############################################################################
    ##    Additional Methods and properties
    ############################################################################
    @property
    def client(self):
        return self
    
    @property
    def prefix(self):
        return ''

    def prefixed(self, prefix):
        '''Return a new :class:`PrefixedRedis` client'''
        return PrefixedRedis(self, prefix)
    
    def execute_script(self, name, keys, *args):
        '''Execute a registered lua script.'''
        script = get_script(name)
        if not script:
            raise RedisError('No such script "%s"' % name)            
        return script(self, keys, *args)
    
    def countpattern(self, pattern):
        "delete all keys matching *pattern*."
        return self.execute_script('countpattern', (), pattern)

    def delpattern(self, pattern):
        "delete all keys matching *pattern*."
        return self.execute_script('delpattern', (), pattern)
    

class RedisProxy(Redis):
    '''A proxy to a :class:`Redis` client. It is the base class
of :class:`PrefixedRedis` and :class:`Pipeline`.

.. attribute:: client

    The underlying :class:`Redis` client
'''
    def __init__(self, client):
        self.__client = client

    @property
    def client(self):
        return self.__client
    
    @property
    def connection_pool(self):
        return self.client.connection_pool

    @property
    def response_callbacks(self):
        return self.client.response_callbacks

    @property
    def response_errbacks(self):
        return self.client.response_errbacks

    @property
    def encoding(self):
        return self.client.encoding


class Pipeline(BasePipeline, RedisProxy):
    
    def __init__(self, client, transaction, shard_hint):
        RedisProxy.__init__(self, client)
        self.transaction = transaction
        self.shard_hint = shard_hint
        self.watching = False
        self.connection = None
        self.reset()
    
    def execute(self, raise_on_error=True):
        return self.connection_pool.request_pipeline(self, raise_on_error)
    
    def super_execute(self, raise_on_error):
        return super(Pipeline, self).execute(raise_on_error)
        
    
class PrefixedRedis(RedisProxy):
    '''A class for a prefixed redis client. It append a prefix to all keys.

.. attribute:: prefix

    The prefix to append to all keys
    
'''    
    EXCLUDE_COMMANDS = frozenset(('BGREWRITEOF', 'BGSAVE', 'CLIENT', 'CONFIG',
                                  'DBSIZE', 'DEBUG', 'DISCARD', 'ECHO',
                                  'EVAL', 'EVALSHA', 'EXEC',
                                  'INFO', 'LASTSAVE', 'PING',
                                  'PSUBSCRIBE', 'PUBLISH', 'PUNSUBSCRIBE',
                                  'QUIT', 'RANDOMKEY', 'SAVE', 'SCRIPT',
                                  'SELECT', 'SHUTDOWN', 'SLAVEOF', 
                                  'SLOWLOG', 'SUBSCRIBE', 'SYNC',
                                  'TIME', 'UNSUBSCRIBE', 'UNWATCH'))
    SPECIAL_COMMANDS = {
        'BITOP': prefix_not_first,
        'BLPOP': prefix_not_last,
        'BRPOP': prefix_not_last,
        'BRPOPLPUSH': prefix_not_last,
        'RPOPLPUSH': prefix_all,
        'DEL': prefix_all,
        'FLUSHDB': lambda prefix, args: raise_error(),
        'FLUSHALL': lambda prefix, args: raise_error(),
        'MGET': prefix_all,
        'MSET': prefix_alternate,
        'MSETNX': prefix_alternate,
        'MIGRATE': prefix_all,
        'RENAME': prefix_all,
        'RENAMENX': prefix_all,
        'SDIFF': prefix_all,
        'SDIFFSTORE': prefix_all,
        'SINTER': prefix_all,
        'SINTERSTORE': prefix_all,
        'SMOVE': prefix_not_last,
        'SORT': prefix_sort,
        'SUNION': prefix_all,
        'SUNIONSTORE': prefix_all,
        'WATCH': prefix_all,
        'ZINTERSTORE': prefix_zinter,
        'ZUNIONSTORE': prefix_zinter
    }
    RESPONSE_CALLBACKS = {
        'KEYS': lambda pfix, response: [r[len(pfix):] for r in response],
        'BLPOP': pop_list_result,
        'BRPOP': pop_list_result
    }
    def __init__(self, client, prefix):
        super(PrefixedRedis, self).__init__(client)
        self.__prefix = prefix
        
    @property
    def prefix(self):
        return self.__prefix
    
    def execute_command(self, cmnd, *args, **options):
        "Execute a command and return a parsed response"
        args, options = self.preprocess_command(cmnd, *args, **options)
        return self.client.execute_command(cmnd, *args, **options)
    
    def preprocess_command(self, cmnd, *args, **options):
        if cmnd not in self.EXCLUDE_COMMANDS:
            handle = self.SPECIAL_COMMANDS.get(cmnd, self.handle)
            args = handle(self.prefix, args)
        return args, options
    
    def handle(self, prefix, args):
        if args:
            args = list(args)
            args[0] = '%s%s' % (prefix, args[0])
        return args
        
    def dbsize(self):
        return self.client.countpattern('%s*' % self.prefix)
    
    def flushdb(self):
        return self.client.delpattern('%s*' % self.prefix)
            
    def _parse_response(self, request, response, command_name, args, options):
        if command_name in self.RESPONSE_CALLBACKS:
            if not isinstance(response, Exception):
                response = self.RESPONSE_CALLBACKS[command_name](self.prefix,
                                                                 response)
        return self.client._parse_response(request, response, command_name,
                                           args, options)
    
