'''
The :mod:`stdnet.lib.redis` module was originally forked from redis-py_
in January 2011. Since than it has moved on a different direction.

Copyright (c)

* 2010 Andy McCurdy. BSD License
* 2011-2012 Luca Sbardella. BSD License


.. _redis-py: https://github.com/andymccurdy/redis-py
'''
import time
from copy import copy
from datetime import datetime
from uuid import uuid4
from functools import partial
from collections import namedtuple

from stdnet.utils import zip, is_int, iteritems, is_string, flat_mapping
from stdnet.utils.dispatch import Signal

from .connection import *
from .exceptions import *

from .scripts import eval_command_callback, get_script, pairs_to_dict,\
                        load_missing_scripts, script_command_callback

redis_command = namedtuple('redis_command','command args options callbacks')

__all__ = ['Redis',
           'RedisProxy',
           'PrefixedRedis',
           'Pipeline']


collection_list = (tuple, list, set, frozenset, dict)


def raise_error(exception=NotImplementedError):
    raise exception()

def list_or_args(keys, args = None):
    if not isinstance(keys, collection_list):
        keys = [keys]
    if args:
        keys.extend(args)
    return keys

def pairs_to_dict_cbk(request, response, args, **options):
    return pairs_to_dict(response, request.client.encoding)

def timestamp_to_datetime(request, response, args, **options):
    "Converts a unix timestamp to a Python datetime object"
    if not response:
        return None
    try:
        response = int(response)
    except ValueError:
        return None
    return datetime.fromtimestamp(response)

def string_keys_to_dict(key_string, callback):
    return dict([(key, callback) for key in key_string.split()])

def dict_merge(*dicts):
    merged = {}
    [merged.update(d) for d in dicts]
    return merged

def parse_info(request, response, args, **options):
    '''Parse the response of Redis's INFO command into a Python dict.
In doing so, convert byte data into unicode.'''
    info = {}
    encoding = request.client.encoding
    response = response.decode(encoding)
    def get_value(value):
        if ',' and '=' not in value:
            return value
        sub_dict = {}
        for item in value.split(','):
            k, v = item.split('=')
            try:
                sub_dict[k] = int(v)
            except ValueError:
                sub_dict[k] = v
        return sub_dict
    data = info
    for line in response.splitlines():
        keyvalue = line.split(':')
        if len(keyvalue) == 2:
            key,value = keyvalue
            try:
                data[key] = int(value)
            except ValueError:
                data[key] = get_value(value)
        else:
            data = {}
            info[line[2:]] = data
    return info

def zset_score_pairs(request, response, args, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a list of (value, score) pairs
    """
    if not response or not options['withscores']:
        return response
    return zip(response[::2], map(float, response[1::2]))

def int_or_none(request, response, args, **options):
    if response is None:
        return None
    return int(response)

def float_or_none(request, response, args, **options):
    if response is None:
        return None
    return float(response)

def bytes_to_string(request, response, args, **options):
    encoding = request.client.encoding
    if isinstance(response,list):
        return (res.decode(encoding) for res in response)
    elif response is not None:
        return response.decode(request.client.encoding)
    else:
        return response

def config_callback(request, response, args, **options):
    if args[0] == 'GET':
        encoding = request.client.encoding
        return dict(((k,v.decode(encoding))\
            for k,v in pairs_to_dict_cbk(request, response, args, **options)))
    else:
        return response == b'OK'

def slowlog_callback(request, response, args, **options):
    if args[0] == 'GET':
        encoding = request.client.encoding
        commands = []
        for id,ts,ms,command in response:
            cmd = command.pop(0)
            commands.append({'id':id,
                             'microseconds':ms,
                             'timestamp':ts,
                             'command': cmd.decode(encoding),
                             'args': tuple(command)
                             })
        return commands
    elif args[0] == 'LEN':
        return response
    else:
        return response == b'OK'


class Redis(object):
    """Implementation of the Redis protocol.
    This class provides a Python interface to all Redis commands.

    Connection and Pipeline derive from this, implementing how
    the commands are sent and received to the Redis server
    """
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            'KEYS RANDOMKEY TYPE OBJECT HKEYS',
            bytes_to_string
            ),
        string_keys_to_dict(
            'AUTH DEL EXISTS EXPIRE EXPIREAT HDEL HEXISTS HMSET MOVE MSETNX '
            'TSEXISTS RENAMENX SADD SISMEMBER SMOVE SETEX SETNX SREM ZADD ZREM',
            lambda request, response, args, **options : bool(response)
            ),
        string_keys_to_dict(
            'DECRBY HLEN INCRBY LLEN SCARD SDIFFSTORE SINTERSTORE '
            'STRLEN SUNIONSTORE ZCARD ZREMRANGEBYSCORE ZREVRANK',
            lambda request, response, args, **options : int(response)
            ),
        string_keys_to_dict(
            # these return OK, or int if redis-server is >=1.3.4
            'LPUSH RPUSH',
            lambda request, response, args, **options:\
                is_int(response) and response or response == b'OK'
            ),
        string_keys_to_dict('ZSCORE ZINCRBY', float_or_none),
        string_keys_to_dict(
            'FLUSHALL FLUSHDB LSET LTRIM MSET RENAME SELECT'
            'SAVE SET SHUTDOWN SLAVEOF WATCH UNWATCH',
            lambda request, response, args, **options: response == b'OK'
            ),
        string_keys_to_dict(
            'BLPOP BRPOP',
            lambda request, response, args, **options:\
                         response and tuple(response) or None
            ),
        string_keys_to_dict('SDIFF SINTER SMEMBERS SUNION',
            lambda request, response, args, **options: set(response)
            ),
        string_keys_to_dict(
            'ZRANGE ZRANGEBYSCORE ZREVRANGE',
            zset_score_pairs
            ),
        {
            'BGREWRITEAOF': lambda request, response, args, **options: \
                response == b'Background rewriting of AOF file started',
            'BGSAVE': lambda request, response, args, **options: \
                response == b'Background saving started',
            'HGETALL': pairs_to_dict_cbk,
            'INFO': parse_info,
            'LASTSAVE': timestamp_to_datetime,
            'PING': lambda request, response, args, **options: \
                response == b'PONG',
            'TTL': lambda request, response, args, **options: \
                response != -1 and response or None,
            'ZRANK': int_or_none,
            'EVALSHA': eval_command_callback,
            'EVAL': eval_command_callback,
            'SCRIPT': script_command_callback,
            'CONFIG': config_callback,
            'SLOWLOG': slowlog_callback
        }
        )

    RESPONSE_ERRBACKS = {
        'EVALSHA': eval_command_callback,
        'EVAL': eval_command_callback
    }
    _STATUS = ''

    def __init__(self, connection_pool=None, **connection_kwargs):
        connection_pool = connection_pool or\
                            ConnectionPool.create(**connection_kwargs)
        self.connection_pool = connection_pool
        self.response_callbacks = self.RESPONSE_CALLBACKS.copy()
        self.response_errbacks = self.RESPONSE_ERRBACKS.copy()

    @property
    def client(self):
        return self

    @property
    def pipelined(self):
        return False
    
    @property
    def prefix(self):
        return ''
        
    @property
    def encoding(self):
        return self.connection_pool.encoding

    def _get_db(self):
        return self.connection_pool.db
    db = property(_get_db)

    def __eq__(self, other):
        return self.connection_pool == other.connection_pool

    def clone(self, **kwargs):
        c = copy(self)
        c.connection_pool = self.connection_pool.clone(**kwargs)
        return c

    def pipeline(self):
        """
Return a new :class:`Pipeline` that can queue multiple commands for
later execution. Apart from making a group of operations
atomic, pipelines are useful for reducing the back-and-forth overhead
between the client and server.
"""
        return Pipeline(self)
    
    def prefixed(self, prefix):
        '''Return a new :class:`PrefixedRedis` client'''
        return PrefixedRedis(self, prefix)

    def preprocess_command(self, cmnd, *args, **options):
        return args, options
        
    def request(self, cmnd, *args, **options):
        '''Return a new :class:`RedisRequest`'''
        connection = self.connection_pool.get_connection()
        args, options = self.preprocess_command(cmnd, *args, **options)
        return connection.request(self, cmnd, *args, **options)
    
    def execute_command(self, *args, **options):
        "Execute a command and return a parsed response"
        return self.request(*args, **options).execute()

    def _parse_response(self, request, response, command_name, args, options):
        callbacks = self.response_errbacks if isinstance(response, Exception)\
                        else self.response_callbacks
        if command_name in callbacks:
            cbk = callbacks[command_name]
            return cbk(request, response, args, **options)
        return response

    def parse_response(self, request):
        "Parses a response from the Redis server"
        return self._parse_response(request,
                                    request.response,
                                    request.command_name,
                                    request.args,
                                    request.options)

    ############################################################################
    ##    SERVER INFORMATION
    ############################################################################
    def bgrewriteaof(self):
        "Tell the Redis server to rewrite the AOF file from data in memory."
        return self.execute_command('BGREWRITEAOF')

    def bgsave(self):
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """
        return self.execute_command('BGSAVE')

    def config_get(self, pattern="*"):
        "Return a dictionary of configuration based on the ``pattern``"
        return self.execute_command('CONFIG', 'GET', pattern, parse='GET')

    def config_set(self, name, value):
        "Set config item ``name`` with ``value``"
        return self.execute_command('CONFIG', 'SET', name, value, parse='SET')

    def dbsize(self):
        "Returns the number of keys in the current database"
        return self.execute_command('DBSIZE')

    def delete(self, *names):
        "Delete one or more keys specified by ``names``"
        return self.execute_command('DEL', *names)
    __delitem__ = delete

    def flushall(self): #pragma    nocover
        "Delete all keys in all databases on the current host"
        return self.execute_command('FLUSHALL')

    def flushdb(self):
        "Delete all keys in the current database"
        return self.execute_command('FLUSHDB')

    def info(self):
        "Returns a dictionary containing information about the Redis server"
        return self.execute_command('INFO')

    def lastsave(self):
        """
        Return a Python datetime object representing the last time the
        Redis database was saved to disk
        """
        return self.execute_command('LASTSAVE')

    def ping(self):
        "Ping the Redis server"
        return self.execute_command('PING')
    
    def echo(self, message):
        "Ping the Redis server"
        return self.execute_command('ECHO', message)

    def save(self):
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete
        """
        return self.execute_command('SAVE')

    def shutdown(self): #pragma    nocover
        "Shutdown the server"
        try:
            self.execute_command('SHUTDOWN')
        except RedisConnectionError:
            # a RedisConnectionError here is expected
            return
        raise RedisError("SHUTDOWN seems to have failed.")

    def slaveof(self, host=None, port=None):
        """Set the server to be a replicated slave of the instance identified
by the ``host`` and ``port``. If called without arguements, the
instance is promoted to a master instead.
"""
        if host is None and port is None:
            return self.execute_command("SLAVEOF", "NO", "ONE")
        return self.execute_command("SLAVEOF", host, port)

    def slowlog_get(self, entries = 1):
        return self.execute_command("SLOWLOG", 'GET', entries)

    def slowlog_len(self):
        return self.execute_command("SLOWLOG", 'LEN')

    def slowlog_reset(self):
        return self.execute_command("SLOWLOG", 'RESET')

    #### BASIC KEY COMMANDS ####
    def append(self, key, value):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.
        """
        return self.execute_command('APPEND', key, value)

    def decr(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """
        return self.execute_command('DECRBY', name, amount)

    def exists(self, name, **options):
        "Returns a boolean indicating whether key ``name`` exists"
        return self.execute_command('EXISTS', name, **options)
    __contains__ = exists

    def expire(self, name, time):
        "Set an expire flag on key ``name`` for ``time`` seconds"
        return self.execute_command('EXPIRE', name, time)

    def expireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer indicating unix time or a Python datetime object.
        """
        if isinstance(when, datetime):
            when = int(time.mktime(when.timetuple()))
        return self.execute_command('EXPIREAT', name, when)

    def get(self, name):
        """
        Return the value at key ``name``, or None of the key doesn't exist
        """
        return self.execute_command('GET', name)
    __getitem__ = get

    def getset(self, name, value):
        """
        Set the value at key ``name`` to ``value`` if key doesn't exist
        Return the value at key ``name`` atomically
        """
        return self.execute_command('GETSET', name, value)

    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        return self.execute_command('INCRBY', name, amount)

    def keys(self, pattern='*'):
        "Returns a list of keys matching ``pattern``"
        return self.execute_command('KEYS', pattern)

    def mget(self, keys, *args):
        """Returns a list of values ordered identically to ``keys``"""
        keys = list_or_args(keys, args)
        return self.execute_command('MGET', *keys)

    def mset(self, mapping):
        "Sets each key in the ``mapping`` dict to its corresponding value"
        items = flat_mapping(mapping)
        return self.execute_command('MSET', *items)

    def msetnx(self, mapping):
        """
        Sets each key in the ``mapping`` dict to its corresponding value if
        none of the keys are already set
        """
        items = flat_mapping(mapping)
        return self.execute_command('MSETNX', *items)

    def move(self, name, db):
        "Moves the key ``name`` to a different Redis database ``db``"
        return self.execute_command('MOVE', name, db)

    def randomkey(self):
        "Returns the name of a random key"
        return self.execute_command('RANDOMKEY')

    def rename(self, src, dst):
        """
        Rename key ``src`` to ``dst``
        """
        return self.execute_command('RENAME', src, dst)

    def renamenx(self, src, dst):
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        return self.execute_command('RENAMENX', src, dst)


    def set(self, name, value, timeout = None):
        """Execute the ``SET`` command to set the value at key ``name``
to ``value``. If a ``timeout`` is available and positive,
the ``SETEX`` command is executed instead."""
        if timeout and timeout > 0:
            return self.execute_command('SETEX', name, timeout, value)
        else:
            return self.execute_command('SET', name, value)
    __setitem__ = set

    def setnx(self, name, value):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"
        return self.execute_command('SETNX', name, value)

    def strlen(self, name):
        "Return the number of bytes stored in the value of ``name``"
        return self.execute_command('STRLEN', name)

    def substr(self, name, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        return self.execute_command('SUBSTR', name, start, end)

    def ttl(self, name):
        "Returns the number of seconds until the key ``name`` will expire"
        return self.execute_command('TTL', name)

    def type(self, name):
        "Returns the type of key ``name``"
        return self.execute_command('TYPE', name)

    def object(self, name, subcommand):
        '''Returns the subcommand on key ``name``. The subcommand
can be one of: refcount, encoding, idletime.'''
        return self.execute_command('OBJECT', subcommand, name)

    #### LIST COMMANDS ####
    def blpop(self, keys, timeout=0, **options):
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        if is_string(keys):
            keys = [keys]
        else:
            keys = list(keys)
        keys.append(timeout)
        return self.execute_command('BLPOP', *keys, **options)

    def brpop(self, keys, timeout=0, **options):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        if is_string(keys):
            keys = [keys]
        else:
            keys = list(keys)
        keys.append(timeout)
        return self.execute_command('BRPOP', *keys, **options)

    def lindex(self, name, index, **options):
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list
        """
        return self.execute_command('LINDEX', name, index, **options)

    def llen(self, name, **options):
        "Return the length of the list ``name``"
        return self.execute_command('LLEN', name, **options)

    def lpop(self, name, **options):
        "Remove and return the first item of the list ``name``"
        return self.execute_command('LPOP', name, **options)

    def lpush(self, name, *values, **options):
        "Push ``values`` onto the head of the list ``name``"
        return self.execute_command('LPUSH', name, *values, **options)

    def lrange(self, name, start, end, **options):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command('LRANGE', name, start, end, **options)

    def lrem(self, name, value, num=0, **options):
        """
        Remove the first ``num`` occurrences of ``value`` from list ``name``

        If ``num`` is 0, then all occurrences will be removed
        """
        return self.execute_command('LREM', name, num, value, **options)

    def lset(self, name, index, value, **options):
        "Set ``position`` of list ``name`` to ``value``"
        return self.execute_command('LSET', name, index, value, **options)

    def ltrim(self, name, start, end, **options):
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command('LTRIM', name, start, end, **options)

    def rpop(self, name, **options):
        "Remove and return the last item of the list ``name``"
        return self.execute_command('RPOP', name, **options)

    def rpoplpush(self, src, dst, **options):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        return self.execute_command('RPOPLPUSH', src, dst, **options)

    def rpush(self, name, *values, **options):
        "Push ``values`` onto the tail of the list ``name``"
        return self.execute_command('RPUSH', name, *values, **options)

    def sort(self, name, start=None, num=None, by=None, get=None,
            desc=False, alpha=False, store=None, storeset=None,
            **options):
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where int he key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")

        pieces = [name]
        if by is not None:
            pieces.append('BY')
            pieces.append(by)
        if start is not None and num is not None:
            pieces.append('LIMIT')
            pieces.append(start)
            pieces.append(num)
        if get is not None:
            if not isinstance(get, (list,tuple)):
                get = (get,)
            for g in get:
                pieces.append('GET')
                pieces.append(g)
        if desc:
            pieces.append('DESC')
        if alpha:
            pieces.append('ALPHA')
        if store is not None:
            pieces.append('STORE')
            pieces.append(store)
        return self.execute_command('SORT', *pieces, **options)

    ############################################################################
    ##    SET COMMANDS
    ############################################################################
    def sadd(self, name, *values, **options):
        return self.execute_command('SADD', name, *values, **options)

    def scard(self, name, **options):
        return self.execute_command('SCARD', name, **options)

    def sdiff(self, keys, *args, **options):
        keys = list_or_args(keys, args)
        return self.execute_command('SDIFF', *keys, **options)

    def sdiffstore(self, dest, keys, *args, **options):
        keys = list_or_args(keys, args)
        return self.execute_command('SDIFFSTORE', dest, *keys, **options)

    def sinter(self, keys, *args, **options):
        keys = list_or_args(keys, args)
        return self.execute_command('SINTER', *keys, **options)

    def sinterstore(self, dest, keys, *args, **options):
        keys = list_or_args(keys, args)
        return self.execute_command('SINTERSTORE', dest, *keys, **options)

    def sismember(self, name, value, **options):
        return self.execute_command('SISMEMBER', name, value, **options)

    def smembers(self, name, **options):
        return self.execute_command('SMEMBERS', name, **options)

    def smove(self, src, dst, value, **options):
        return self.execute_command('SMOVE', src, dst, value, **options)

    def spop(self, name, **options):
        return self.execute_command('SPOP', name, **options)

    def srandmember(self, name, **options):
        return self.execute_command('SRANDMEMBER', name, **options)

    def srem(self, name, *values, **options):
        return self.execute_command('SREM', name, *values, **options)

    def sunion(self, keys, *args, **options):
        keys = list_or_args(keys, args)
        return self.execute_command('SUNION', *keys, **options)

    def sunionstore(self, dest, keys, *args, **options):
        keys = list_or_args(keys, args)
        return self.execute_command('SUNIONSTORE', dest, *keys, **options)

    ############################################################################
    ##    SORTED SET
    ############################################################################
    def zadd(self, name, *args, **options):
        '''Add member the iterable over two dimensional elements ``args``.
The first element is the score and the second is the value.'''
        if len(args) % 2:
            raise RedisError(\
                    "ZADD requires an equal number of values and scores")
        return self.execute_command('ZADD', name, *args, **options)

    def zcard(self, name, **options):
        "Return the number of elements in the sorted set ``name``"
        return self.execute_command('ZCARD', name, **options)

    def zincrby(self, name, value, amount=1, **options):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        return self.execute_command('ZINCRBY', name, amount, value, **options)

    def zrange(self, name, start, end, desc=False, withscores=False, **options):
        """Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` indicates to sort in descending order.

        ``withscores`` indicates to return the scores along with the values.
            The return type is a list of (value, score) pairs
        """
        command = 'ZREVRANGE' if desc else 'ZRANGE'
        pieces = [command, name, start, end]
        if withscores:
            pieces.append('withscores')
        options['withscores'] = withscores
        return self.execute_command(*pieces, **options)

    def zrangebyscore(self, name, min, max, desc = False,
                      start=None, num=None, withscores=False,
                      **options):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the range.

        ``withscores`` indicates to return the scores along with the values.
            The return type is a list of (value, score) pairs
        """
        command = 'ZREVRANGEBYSCORE' if desc else 'ZRANGEBYSCORE'
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")
        pieces = [command, name, min, max]
        if start is not None and num is not None:
            pieces.extend(['LIMIT', start, num])
        if withscores:
            pieces.append('withscores')
        options['withscores'] = withscores
        return self.execute_command(*pieces, **options)

    def zrank(self, name, value, **options):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        return self.execute_command('ZRANK', name, value, **options)

    def zrem(self, name, *values, **options):
        "Remove member ``value`` from sorted set ``name``"
        return self.execute_command('ZREM', name, *values, **options)

    def zremrangebyrank(self, name, start, stop, **options):
        ''''Remove all elements in the sorted set ``name`` with rank
        between ``start`` and ``stop``.
        '''
        return self.execute_command('ZREMRANGEBYRANK', name, start, stop,
                                    **options)

    def zremrangebyscore(self, name, min, max, **options):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``
        """
        return self.execute_command('ZREMRANGEBYSCORE', name, min, max,
                                    **options)

    def zrevrank(self, name, value, **options):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return self.execute_command('ZREVRANK', name, value, **options)

    def zscore(self, name, value, **options):
        "Return the score of element ``value`` in sorted set ``name``"
        return self.execute_command('ZSCORE', name, value, **options)

    def zinterstore(self, dest, keys, *args, **options):
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        keys = list_or_args(keys, args)
        return self._zaggregate('ZINTERSTORE', dest, keys, **options)

    def zunionstore(self, dest, keys, *args, **options):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        keys = list_or_args(keys, args)
        return self._zaggregate('ZUNIONSTORE', dest, keys, **options)

    def zdiffstore(self, dest, keys, *args, **options):
        """
        Compute the difference of multiple sorted sets specified by
        ``keys`` into a new sorted set, ``dest``.
        """
        keys = (dest,) + tuple(list_or_args(keys, args))
        withscores = options.pop('withscores', False)
        if withscores:
            withscores = 'withscores'
        else:
            withscores = ''
        return self.script_call('zdiffstore', keys, withscores, **options)

    # zset script commands

    def zpopbyrank(self, name, start, stop=None, withscores=False,
                   desc=False, **options):
        '''Pop a range by rank'''
        options['withscores'] = withscores
        stop = stop if stop is not None else start
        return self.script_call('zpop', (name,), 'rank',
                                start, stop, int(desc), int(withscores),
                                **options)

    def zpopbyscore(self, name, start, stop=None, withscores=False,
                    desc=False, **options):
        '''Pop a range by score'''
        options['withscores'] = withscores
        stop = stop if stop is not None else start
        return self.script_call('zpop', (name,), 'score',
                                start, stop, int(desc), int(withscores),
                                **options)

    def _zaggregate(self, command, dest, keys,
                    aggregate=None, withscores=None, **options):
        pieces = [command, dest, len(keys)]
        if isinstance(keys, dict):
            items = keys.items()
            keys = [i[0] for i in items]
            weights = [i[1] for i in items]
        else:
            weights = None
        pieces.extend(keys)
        if weights:
            pieces.append('WEIGHTS')
            pieces.extend(weights)
        if aggregate:
            pieces.append('AGGREGATE')
            pieces.append(aggregate)
        if withscores:
            pieces.append('WITHSCORES')
            pieces.append(withscores)
        return self.execute_command(*pieces, **options)

    ############################################################################
    ##    HASH COMMANDS
    ############################################################################
    def hdel(self, key, field, *fields):
        '''Removes the specified ``fields`` from the hash stored at ``key``'''
        return self.execute_command('HDEL', key, field, *fields)

    def hexists(self, name, key):
        "Returns a boolean indicating if ``key`` exists within hash ``name``"
        return self.execute_command('HEXISTS', name, key)

    def hget(self, name, key):
        "Return the value of ``key`` within the hash ``name``"
        return self.execute_command('HGET', name, key)

    def hgetall(self, name):
        "Return a Python dict of the hash's name/value pairs"
        return self.execute_command('HGETALL', name)

    def hincrby(self, name, key, amount=1):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"
        return self.execute_command('HINCRBY', name, key, amount)

    def hkeys(self, name):
        "Return the list of keys within hash ``name``"
        return self.execute_command('HKEYS', name)

    def hlen(self, name):
        "Return the number of elements in hash ``name``"
        return self.execute_command('HLEN', name)

    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        return self.execute_command('HSET', name, key, value)

    def hsetnx(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        return self.execute_command("HSETNX", name, key, value)

    def hmset(self, name, mapping):
        """
        Sets each key in the ``mapping`` dict to its corresponding value
        in the hash ``name``
        """
        items = flat_mapping(mapping)
        return self.execute_command('HMSET', name, *items)

    def hmget(self, name, *fields):
        "Returns a list of values ordered identically to ``fields``"
        return self.execute_command('HMGET', name, *fields)

    def hvals(self, name):
        "Return the list of values within hash ``name``"
        return self.execute_command('HVALS', name)

    ############################################################################
    ##    Scripting
    ############################################################################
    def _eval(self, command, body, keys, *args, **options):
        if keys:
            if not isinstance(keys, collection_list):
                params = (keys,)
            else:
                params = tuple(keys)
            num_keys = len(params)
            params = params + args
        else:
            num_keys = 0
            params = args
        return self.execute_command(command, body, num_keys, *params, **options)

    def eval(self, body, keys, *args, **options):
        return self._eval('EVAL', body, keys, *args, **options)

    def evalsha(self, body, keys, *args, **options):
        return self._eval('EVALSHA', body, keys, *args, **options)

    def script_call(self, name, keys, *args, **options):
        '''Execute a registered lua script.'''
        script = get_script(name)
        if not script:
            raise RedisError('No such script {0}'.format(name))
        return script.evalsha(self, keys, *args, **options)

    def script_flush(self):
        return self.execute_command('SCRIPT', 'FLUSH', command='FLUSH')

    def script_load(self, script, script_name=None):
        return self.execute_command('SCRIPT', 'LOAD', script, command='LOAD',
                                    script_name=script_name)

    ############################################################################
    ##    Script commands
    ############################################################################
    def countpattern(self, pattern):
        "delete all keys matching *pattern*."
        return self.script_call('countpattern', (), pattern)

    def delpattern(self, pattern):
        "delete all keys matching *pattern*."
        return self.script_call('delpattern', (), pattern)


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

    def clone(self, **kwargs):
        c = copy(self)
        c.__client = c.client.clone(**kwargs)
        return c
        

prefix_all = lambda pfix, args: ['%s%s' % (pfix, a) for a in args]
prefix_alternate = lambda pfix, args: [a if n//2*2==n else '%s%s' % (pfix, a)\
                                       for n, a in enumerate(args,1)]
prefix_not_last = lambda pfix, args: ['%s%s' % (pfix, a) for a in args[:-1]]\
                                        + [args[-1]]
prefix_not_first = lambda pfix, args: [args[0]] +\
                                      ['%s%s' % (pfix, a) for a in args[1:]]

def prefix_zinter(pfix, args):
    dest, numkeys, params = args[0], args[1], args[2:]
    args = ['%s%s' % (pfix, dest), numkeys]
    nk = 0
    for p in params:
        if nk < numkeys:
            nk += 1
            p = '%s%s' % (pfix, p)
        args.append(p)
    return args

def prefix_sort(pfix, args):
    prefix = True
    nargs = []
    for a in args:
        if prefix:
            a = '%s%s' % (pfix, a)
            prefix = False
        elif a in ('BY', 'GET', 'STORE'):
            prefix = True
        nargs.append(a)
    return nargs
    
def pop_list_result(pfix, result):
    if result:
        return (result[0][len(pfix):], result[1])


class PrefixedRedis(RedisProxy):
    '''A :class:`RedisProxy` for a prefixed redis client.
It append a prefix to all keys.

.. attribute:: prefix

    The prefix for this :class:`PrefixedRedis`
    
Typical usage::

    >>> from stdnet import getdb
    >>> redis = getdb().client
    >>> pr = redis.prefixed('myprefix.')
    >>> pr.set('a','foo')
    True
    >>> pr.get('a')
    'foo'
    >>> redis.get('a')
    None
    >>> redis.get('myprefix.a')
    'foo'
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
    
    def preprocess_command(self, cmnd, *args, **options):
        if args and cmnd not in self.EXCLUDE_COMMANDS:
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
    
    def _eval(self, command, body, keys, *args, **options):
        if keys:
            keys = ['%s%s' % (self.prefix, k) for k in keys]
        return super(PrefixedRedis, self)._eval(command, body, keys, *args,
                                                **options)
        
    def _parse_response(self, request, response, command_name, args, options):
        if command_name in self.RESPONSE_CALLBACKS:
            if not isinstance(response, Exception):
                response = self.RESPONSE_CALLBACKS[command_name](self.prefix,
                                                                 response)
        return self.client._parse_response(request, response, command_name,
                                           args, options)
    
        
class Pipeline(RedisProxy):
    """A :class:`Pipeline` provide a way to commit multiple commands
to the Redis server in one transmission.
This is convenient for batch processing, such as
saving all the values in a list to Redis.

All commands executed within a pipeline are wrapped with MULTI and EXEC
calls. This guarantees all commands executed in the pipeline will be
executed atomically.

Check `redis transactions <http://redis.io/topics/transactions>`_
for further information.

Any command raising an exception does *not* halt the execution of
subsequent commands in the pipeline. Instead, the exception is caught
and its instance is placed into the response list returned by
:meth:`execute`.
Code iterating over the response list should be able to deal with an
instance of an exception as a potential value. In general, these will be
ResponseError exceptions, such as those raised when issuing a command
on a key of a different datatype.
"""
    def __init__(self, client):
        super(Pipeline,self).__init__(client)
        self.reset()

    def reset(self):
        self.command_stack = []
        self.execute_command('MULTI')

    @property
    def pipelined(self):
        return True
    
    @property
    def empty(self):
        return len(self.command_stack) <= 1

    def execute_command(self, cmnd, *args, **options):
        """
Stage a command to be executed when execute() is next called

Returns the current Pipeline object back so commands can be
chained together, such as:

pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

At some other point, you can then run: pipe.execute(),
which will execute all commands queued in the pipe.
"""
        callbacks = []
        args, options = self.client.preprocess_command(cmnd, *args, **options)
        self.command_stack.append(
                        redis_command(cmnd, tuple(args), options, callbacks))
        return self

    def add_callback(self, callback):
        '''Adding a callback to the latest command in the pipeline.
Typical usage::

    pipe.sadd('foo').add_callback(mycallback)

The callback will be executed after the default callback for the command
with the following syntax::

    mycallback(results, current_result)

where ``results`` is a list of previous pipeline's command results and
``current_result`` is the result of the command the callback is
associated with. The result from the callback will be added to
the list of results.
Several callbacks can be added for a given command::

    pipe.sadd('foo').add_callback(mycallback).add_callback(mycallback2)
'''
        if self.empty:
            raise RedisError('Cannot add callback. No command in the stack')
        self.command_stack[-1].callbacks.append(callback)
        return self

    def parse_response(self, request):
        response = request.response[-1]
        commands = request.args[1:-1]
        if len(response) != len(commands):
            raise ResponseError("Wrong number of response items from "
                                "pipeline execution")
        processed = []
        parse_response = self._parse_response
        for r, cmd in zip(response, commands):
            command, args, options, callbacks = cmd
            r = parse_response(request, r, command, args, options)
            for callback in callbacks:
                r = callback(processed, r)
            processed.append(r)
        if request.load_script:
            return load_missing_scripts(self, commands, processed)
        else:
            return processed

    def request(self, load_script=False):
        self.execute_command('EXEC')
        commands = self.command_stack
        self.reset()
        conn = self.connection_pool.get_connection()
        request = conn.request(self, None, *commands)
        request.load_script = load_script
        return request
        
    def execute(self, load_script=False):
        '''Execute all commands in the current pipeline.'''
        return self.request(load_script).execute()
