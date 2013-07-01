'''The :mod:`stdnet.backends.redisb.client` implements several extensions
to the standard redis client in redis-py_


Client
~~~~~~~~~~~~~~

.. autoclass:: Redis
   :members:
   :member-order: bysource
   
Prefixed Client
~~~~~~~~~~~~~~~~~~

.. autoclass:: PrefixedRedis
   :members:
   :member-order: bysource
   
RedisScript
~~~~~~~~~~~~~~~

.. autoclass:: RedisScript
   :members:
   :member-order: bysource
     
'''
import os
import io
import socket
from copy import copy
from itertools import chain

try:
    import redis
except ImportError:
    from stdnet import ImproperlyConfigured
    raise ImproperlyConfigured('Redis backend requires redis python client')

from redis.exceptions import NoScriptError, ConnectionError, ResponseError,\
                                RedisError
from redis.client import pairs_to_dict, BasePipeline, list_or_args, PubSub
from redis.connection import PythonParser

from .extensions import get_script, RedisManager, CppRedisManager, RedisScript,\
                        script_callback, redis_connection, redis_after_receive
from .prefixed import *
from .info import parse_info

__all__ = ['pairs_to_dict',
           'Redis',
           'ConnectionPool',
           'RedisError',
           'CppConnectionPool']


class Connection(redis.Connection):
    
    def read_response(self):
        "Read the response from a previously sent command"
        try:
            response = self._read_response()
        except Exception:
            self.disconnect()
            raise
        if isinstance(response, ResponseError):
            raise response
        return response
    
    def _read_response(self):
        if not self._parser:
            raise ConnectionError("Socket closed on remote end")
        response = self._parser.get()
        while response is False:
            try:
                buffer = self._sock.recv(4096)
            except (socket.error, socket.timeout) as e:
                raise ConnectionError("Error while reading from socket: %s" %
                                      (e.args,))
            if not buffer:
                raise ConnectionError("Socket closed on remote end")
            self._parser.feed(buffer)
            response = self._parser.get()
        return response
     

class ConnectionPoolBase(redis.ConnectionPool):
    '''Synchronous Redis connection pool compatible with the Asynchronous One'''
    def __init__(self, address, db=0, parser=None, **kwargs):
        if isinstance(address, tuple):
            host, port = address
            kwargs['host'] = host
            kwargs['port'] = port
            address = (host, port)
        else:
            kwargs['path'] = address
            address = address
        self._setup(address, db, parser)
        kwargs['connection_class'] = Connection
        kwargs['parser_class'] = parser
        super(ConnectionPoolBase, self).__init__(db=self.db, **kwargs)
    
    def _checkpid(self):
        if self.pid != os.getpid():
            self.disconnect()
            self.pid = os.getpid()
            self._created_connections = 0

    @property
    def encoding(self):
        return self.connection_kwargs.get('encoding') or 'utf-8'
    
    @property
    def encoding_errors(self):
        return self.connection_kwargs.get('encoding_errors') or 'strict'

    def pubsub(self, shard_hint=None):
        return PubSub(self, shard_hint)
    
    def clone(self, db=0):
        if self.db != db:
            params = self.connection_kwargs.copy()
            params.pop('db', None)
            return self.__class__(self.address, db=db, parser=self.redis_parser,
                                  **params)
        else:
            return self
        
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
            
    def request_pipeline(self, pipeline, raise_on_error=True):
        if pipeline.scripts:
            pipeline.load_scripts()
        commands = pipeline.command_stack
        if not commands:
            return ()
        if pipeline.is_transaction:
            commands = list(chain([(('MULTI', ), {})], commands,
                                  [(('EXEC', ), {})]))
        conn = pipeline.connection
        if not conn:
            conn = self.get_connection('MULTI', pipeline.shard_hint)
            pipeline.connection = conn
        try:
            all_cmds = self.pack_pipeline(commands)
            return pipeline.send_commands(all_cmds, commands, raise_on_error)
        except ConnectionError:
            conn.disconnect()
            # if we were watching a variable, the watch is no longer valid
            # since this connection has died. raise a WatchError, which
            # indicates the user should retry his transaction. If this is more
            # than a temporary failure, the WATCH that the user next issue
            # will fail, propegating the real ConnectionError
            if pipeline.watching:
                raise WatchError("A ConnectionError occured on while watching "
                                 "one or more keys")
            # otherwise, it's safe to retry since the transaction isn't
            # predicated on any state
            return pipeline.send_commands(all_cmds, commands, raise_on_error)
        except:
            pipeline.reset()
            raise
        
        
class ConnectionPool(ConnectionPoolBase, RedisManager):
    pass


class CppConnectionPool(ConnectionPoolBase, CppRedisManager):
    pass
        

def dict_update(original, data):
    target = original.copy()
    target.update(data)
    return target
    
class Redis(redis.StrictRedis):
    # Overridden callbacks
    RESPONSE_CALLBACKS = dict_update(
        redis.StrictRedis.RESPONSE_CALLBACKS,
        {'EVALSHA': script_callback,
         'INFO': parse_info}
    )

    def execute_command(self, *args, **options):
        "Execute a command and return a parsed response"
        try:
            return self.connection_pool.request(self, *args, **options)
        except NoScriptError:
            self.connection_pool.clear_scripts()
            raise
    
    def pipeline(self, transaction=True, shard_hint=None):
        return Pipeline(self, transaction, shard_hint)
    
    def parse_response(self, connection, command_name, **options):
        "Parses a response from the Redis server"
        response = connection.read_response()
        if command_name == 'SCRIPT' and options['parse'] == 'FLUSH':
            self.connection_pool.clear_scripts()
        if command_name in self.response_callbacks:
            return self.response_callbacks[command_name](response, **options)
        return response

    def pubsub(self, shard_hint=None):
        return self.connection_pool.pubsub(shard_hint)
    
    ############################################################################
    ##    Additional Methods and properties
    ############################################################################
    @property
    def db(self):
        return self.connection_pool.db
    
    @property
    def encoding(self):
        return self.connection_pool.encoding
    
    @property
    def client(self):
        return self
    
    @property
    def prefix(self):
        return ''
    
    @property
    def is_pipeline(self):
        return False
        
    def on_response(self, result, raise_on_error):
        result = result[0]
        if isinstance(result, Exception) and raise_on_error:
            raise result
        return result
    
    def clone(self, **kwargs):
        c = copy(self)
        c.connection_pool = self.connection_pool.clone(**kwargs)
        return c
    
    def prefixed(self, prefix):
        '''Return a new :class:`PrefixedRedis` client'''
        return PrefixedRedis(self, prefix)
    
    def execute_script(self, name, keys, *args, **options):
        '''Execute a registered lua script at ``name``. The script must
be implemented via subclassing :class:`RedisScript`.

:param name: the name of the registered script.
:param keys: tuple/list of keys pased to the script.
:param args: argument passed to the script.
:param options: key-value parameters passed to the :meth:`RedisScript.callback`
    method once the script has finished execution.
'''
        script = get_script(name)
        if not script:
            raise RedisError('No such script "%s"' % name)            
        return script(self, keys, *args, **options)
    
    def countpattern(self, pattern):
        '''delete all keys matching *pattern*.'''
        return self.execute_script('countpattern', (), pattern)

    def delpattern(self, pattern):
        '''delete all keys matching *pattern*.'''
        return self.execute_script('delpattern', (), pattern)
    
    def zdiffstore(self, dest, keys, withscores=False):
        '''Compute the difference of multiple sorted sets specified by
``keys`` into a new sorted set, ``dest``.'''
        keys = (dest,) + tuple(keys)
        wscores = 'withscores' if withscores else ''
        return self.execute_script('zdiffstore', keys, wscores,
                                   withscores=withscores)
    
    def zpopbyrank(self, name, start, stop=None, withscores=False, desc=False):
        '''Pop a range by rank'''
        stop = stop if stop is not None else start
        return self.execute_script('zpop', (name,), 'rank', start,
                                   stop, int(desc), int(withscores),
                                   withscores=withscores)

    def zpopbyscore(self, name, start, stop=None, withscores=False, desc=False):
        '''Pop a range by score'''
        stop = stop if stop is not None else start
        return self.execute_script('zpop', (name,), 'score', start,
                                   stop, int(desc), int(withscores),
                                   withscores=withscores)

class RedisProxy(Redis):
    '''A proxy to a :class:`Redis` client. It is the base class
of :class:`PrefixedRedis` and :class:`Pipeline`.

.. attribute:: client

    The underlying :class:`Redis` client
'''
    def __init__(self, client):
        self._client = client

    def clone(self, **kwargs):
        c = copy(self)
        c._client = c._client.clone(**kwargs)
        return c
    
    @property
    def client(self):
        return self._client
    
    @property
    def connection_pool(self):
        return self.client.connection_pool

    @property
    def response_callbacks(self):
        return self.client.response_callbacks


class Pipeline(BasePipeline, RedisProxy):
    
    def __init__(self, client, transaction, shard_hint):
        RedisProxy.__init__(self, client)
        self.transaction = transaction
        self.shard_hint = shard_hint
        self.watching = False
        self.connection = None
        self.reset()

    @property
    def is_pipeline(self):
        return True
    
    @property
    def is_transaction(self):
        return self.transaction or self.explicit_transaction
        
    def immediate_execute_command(self, *args, **options):
        raise NotImplementedError
    
    def execute(self, raise_on_error=True):
        #return super(Pipeline, self).execute(raise_on_error=True)
        return self.connection_pool.request_pipeline(self,
                                                raise_on_error=raise_on_error)
    
    def send_commands(self, all_cmds, commands, raise_on_error):
        connection = self.connection
        connection.send_packed_command(all_cmds)
        response = [self.parse_response(connection, args[0], **options)
                        for args, options in commands]
        return self.on_response(response, raise_on_error)
    
    def parse_response(self, connection, command_name, **options):
        if self.is_transaction:
            if command_name != 'EXEC':
                command_name = '_'
            else:
                response = connection.read_response()
                data = []
                for r, cmd in zip(response, self.command_stack):
                    if not isinstance(r, Exception):
                        args, opt = cmd
                        command_name = args[0]
                        if command_name in self.response_callbacks:
                            r = self.response_callbacks[command_name](r, **opt)
                    elif isinstance(r, NoScriptError):
                        self.connection_pool.clear_scripts()
                    data.append(r)
                return data
        return super(Pipeline, self).parse_response(connection, command_name,
                                                    **options)
    
    def on_response(self, results, raise_on_error):
        redis_after_receive.send_robust(Redis, result=results)
        if self.is_transaction:
            results = results[-1]
        try:
            if raise_on_error:
                self.raise_first_error(results)
            return results
        finally:
            self.reset()
        
    
class PrefixedRedis(RedisProxy):
    '''A class for a prefixed redis client. It append a prefix to all keys.

.. attribute:: prefix

    The prefix to append to all keys
    
'''    
    EXCLUDE_COMMANDS = frozenset(('BGREWRITEOF', 'BGSAVE', 'CLIENT', 'CONFIG',
                                  'DBSIZE', 'DEBUG', 'DISCARD', 'ECHO', 'EXEC',
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
        'EVAL': prefix_eval_keys,
        'EVALSHA': prefix_eval_keys,
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
    
