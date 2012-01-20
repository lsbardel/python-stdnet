'''
The :mod:`stdnet.lib.redis` module was originally forked from redis-py_
in January 2011. Since than it has moved on a different direction.

Copyright (c)

* 2010 Andy McCurdy. BSD License   
* 2011-2012 Luca Sbardella. BSD License
    
    
.. _redis-py: https://github.com/andymccurdy/redis-py
'''
import time
from datetime import datetime
from functools import partial

from stdnet.utils import zip, is_int, iteritems, is_string
from stdnet.utils.dispatch import Signal

from .connection import ConnectionPool
from .exceptions import *

from .scripts import nil, script_call_back, get_script, pairs_to_dict


tuple_list = (tuple, list, set, frozenset)


def list_or_args(keys, args = None):
    if not isinstance(keys, tuple_list):
        keys = [keys]
    if args:
        keys.extend(args)
    return keys


def timestamp_to_datetime(response):
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


def flat_mapping(mapping):
    if isinstance(mapping,dict):
        mapping = iteritems(mapping)
    items = []
    extend = items.extend
    for pair in mapping:
        extend(pair)
    return items

        
def dict_merge(*dicts):
    merged = {}
    [merged.update(d) for d in dicts]
    return merged


def parse_info(response, encoding = 'utf-8'):
    '''Parse the result of Redis's INFO command into a Python dict.
In doing so, convert byte data into unicode.'''
    info = {}
    response = response.decode(encoding)
    def get_value(value):
        if ',' not in value:
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


def ts_pairs(response, withtimes = False, novalues = False, single = False,
             **options):
    '''Parse the timeseries TSRANGE and TSRANGEBYTIME command'''
    if not response:
        return response
    elif withtimes:
        times = (float(t) for t in response[::2])
        return zip(times, response[1::2])
    elif options.get('single') and len(response) == 1:
        return response[0]
    else:
        return response
    
    
def zset_score_pairs(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a list of (value, score) pairs
    """
    if not response or not options['withscores']:
        return response
    return zip(response[::2], map(float, response[1::2]))


def int_or_none(response):
    if response is None:
        return None
    return int(response)


def float_or_none(response):
    if response is None:
        return None
    return float(response)


def bytes_to_string(result, encoding = 'utf-8'):
    if isinstance(result,list):
        return (res.decode(encoding) for res in result)
    elif result is not None:
        return result.decode(encoding)
    else:
        return result


class Redis(object):
    """Implementation of the Redis protocol.
    This class provides a Python interface to all Redis commands.

    Connection and Pipeline derive from this, implementing how
    the commands are sent and received to the Redis server
    """
    NO_KEY_COMMAND = frozenset((
        'SUBSCRIBE', 'PSUBSCRIBE', 'UNSUBSCRIBE', 'PUNSUBSCRIBE',
        'EVAL',
        #
        'AUTH','ECHO','PING','QUIT','SELECT',
        #
        'RANDOMKEY', 'BGREWRITEAOF', 'BGSAVE', 'CONFIG GET', 'CONFIG SET',
        'CONFIG RESETSTAT', 'DBSIZE', 'DEBUG OBJECT', 'DEBUG SEGFAULT',
        'FLUSHALL', 'FLUSHDB', 'INFO', 'LASTSAVE', 'MONITOR', 'SAVE',
        'SHUTDOWN','SLAVEOF','SLOWLOG','SYNC'))
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            'KEYS RANDOMKEY TYPE OBJECT HKEYS',
            bytes_to_string
            ),
        string_keys_to_dict(
            'AUTH DEL EXISTS EXPIRE EXPIREAT HDEL HEXISTS HMSET MOVE MSETNX '
            'TSEXISTS RENAMENX SADD SISMEMBER SMOVE SETEX SETNX SREM ZADD ZREM',
            bool
            ),
        string_keys_to_dict(
            'DECRBY HLEN INCRBY LLEN SCARD SDIFFSTORE SINTERSTORE TSLEN '
            'STRLEN SUNIONSTORE ZCARD ZREMRANGEBYSCORE ZREVRANK',
            int
            ),
        string_keys_to_dict(
            # these return OK, or int if redis-server is >=1.3.4
            'LPUSH RPUSH',
            lambda r: is_int(r) and r or r == OK
            ),
        string_keys_to_dict('ZSCORE ZINCRBY', float_or_none),
        string_keys_to_dict(
            'FLUSHALL FLUSHDB LSET LTRIM MSET RENAME '
            'SAVE SELECT SET SHUTDOWN SLAVEOF WATCH UNWATCH',
            lambda r: r == OK
            ),
        string_keys_to_dict('BLPOP BRPOP', lambda r: r and tuple(r) or None),
        string_keys_to_dict('SDIFF SINTER SMEMBERS SUNION',
            lambda r: set(r)
            ),
        string_keys_to_dict('ZRANGE ZRANGEBYSCORE ZREVRANGE', zset_score_pairs),
        string_keys_to_dict('TSRANGE TSRANGEBYTIME', ts_pairs),
        {
            'BGREWRITEAOF': lambda r: \
                r == 'Background rewriting of AOF file started',
            'BGSAVE': lambda r: r == 'Background saving started',
            'HGETALL': lambda r: pairs_to_dict(r),
            'INFO': parse_info,
            'LASTSAVE': timestamp_to_datetime,
            'PING': lambda r: r == b'PONG',
            'TTL': lambda r: r != -1 and r or None,
            'ZRANK': int_or_none,
            'EVAL': script_call_back
        }
        )

    # commands that should NOT pull data off the network buffer when executed
    SUBSCRIPTION_COMMANDS = set((b'SUBSCRIBE', b'UNSUBSCRIBE'))

    def __init__(self, host='localhost', port=6379,
                 db=0, password=None, socket_timeout=None,
                 connection_pool=None, encoding = 'utf-8',
                 prefix = ''):
        if not connection_pool:
            kwargs = {
                'db': db,
                'password': password,
                'socket_timeout': socket_timeout,
                'host': host,
                'port': port,
                'encoding': encoding
                }
            connection_pool = ConnectionPool(**kwargs)
        self.prefix = prefix
        self.connection_pool = connection_pool
        self.encoding = self.connection_pool.encoding
        self.response_callbacks = self.RESPONSE_CALLBACKS.copy()
        self.signal_on_send = Signal()
        self.signal_on_received = Signal()

    def _get_db(self):
        return self.connection_pool.db
    db = property(_get_db)
    
    def __eq__(self, other):
        return self.connection_pool == other.connection_pool
    
    def pipeline(self):
        """
Return a new :class:`Pipeline` that can queue multiple commands for
later execution. Apart from making a group of operations
atomic, pipelines are useful for reducing the back-and-forth overhead
between the client and server.
"""
        return Pipeline(self.connection_pool,
                        self.response_callbacks,
                        self.signal_on_send,
                        self.signal_on_received)

    #### COMMAND EXECUTION AND PROTOCOL PARSING ####
    def execute_command(self, *args, **options):
        "Execute a command and return a parsed response"
        connection = self.connection_pool.get_connection()
        return connection.execute_command(self.parse_response, *args, **options)

    def parse_response(self, response, command_name, **options):
        "Parses a response from the Redis server"
        if command_name in self.response_callbacks:
            return self.response_callbacks[command_name](response, **options)
        return response

    #### SERVER INFORMATION ####
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

    def flushall(self):
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

    def save(self):
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete
        """
        return self.execute_command('SAVE')
    
    def shutdown(self):
        "Shutdown the server"
        try:
            self.execute_command('SHUTDOWN')
        except ConnectionError:
            # a ConnectionError here is expected
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

    def exists(self, name):
        "Returns a boolean indicating whether key ``name`` exists"
        return self.execute_command('EXISTS', name)
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
        return self.execute_command('KEYS', pattern, encoding = self.encoding)

    def mget(self, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``

        * Passing *args to this method has been deprecated *
        """
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


    def set(self, name, value):
        """
        Set the value at key ``name`` to ``value``
        """
        return self.execute_command('SET', name, value)
    __setitem__ = set

    def setex(self, name, value, time):
        """
        Set the value of key ``name`` to ``value``
        that expires in ``time`` seconds
        """
        return self.execute_command('SETEX', name, time, value)

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
    def blpop(self, keys, timeout=0):
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
        return self.execute_command('BLPOP', *keys)

    def brpop(self, keys, timeout=0):
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
        return self.execute_command('BRPOP', *keys)

    def lindex(self, name, index):
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list
        """
        return self.execute_command('LINDEX', name, index)

    def llen(self, name):
        "Return the length of the list ``name``"
        return self.execute_command('LLEN', name)

    def lpop(self, name):
        "Remove and return the first item of the list ``name``"
        return self.execute_command('LPOP', name)

    def lpush(self, name, *values):
        "Push ``values`` onto the head of the list ``name``"
        return self.execute_command('LPUSH', name, *values)

    def lrange(self, name, start, end):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command('LRANGE', name, start, end)

    def lrem(self, name, value, num=0):
        """
        Remove the first ``num`` occurrences of ``value`` from list ``name``

        If ``num`` is 0, then all occurrences will be removed
        """
        return self.execute_command('LREM', name, num, value)

    def lset(self, name, index, value):
        "Set ``position`` of list ``name`` to ``value``"
        return self.execute_command('LSET', name, index, value)

    def ltrim(self, name, start, end):
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command('LTRIM', name, start, end)

    def rpop(self, name):
        "Remove and return the last item of the list ``name``"
        return self.execute_command('RPOP', name)

    def rpoplpush(self, src, dst):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        return self.execute_command('RPOPLPUSH', src, dst)

    def rpush(self, name, *values):
        "Push ``values`` onto the tail of the list ``name``"
        return self.execute_command('RPUSH', name, *values)

    def sort(self, name, start=None, num=None, by=None, get=None,
            desc=False, alpha=False, store=None, storeset=None):
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
        if storeset is not None:
            pieces.append('STORESET')
            pieces.append(storeset)
        elif store is not None:
            pieces.append('STORE')
            pieces.append(store)
        return self.execute_command('SORT', *pieces)


    #### SET COMMANDS ####
    def sadd(self, name, *values):
        "Add ``value`` to set ``name``"
        return self.execute_command('SADD', name, *values)

    def scard(self, name):
        "Return the number of elements in set ``name``"
        return self.execute_command('SCARD', name)

    def sdiff(self, keys, *args):
        "Return the difference of sets specified by ``keys``"
        keys = list_or_args(keys, args)
        return self.execute_command('SDIFF', *keys)

    def sdiffstore(self, dest, keys, *args):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        keys = list_or_args(keys, args)
        return self.execute_command('SDIFFSTORE', dest, *keys)

    def sinter(self, keys, *args):
        "Return the intersection of sets specified by ``keys``"
        keys = list_or_args(keys, args)
        return self.execute_command('SINTER', *keys)

    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        keys = list_or_args(keys, args)
        return self.execute_command('SINTERSTORE', dest, *keys)

    def sismember(self, name, value):
        "Return a boolean indicating if ``value`` is a member of set ``name``"
        return self.execute_command('SISMEMBER', name, value)

    def smembers(self, name):
        "Return all members of the set ``name``"
        return self.execute_command('SMEMBERS', name)

    def smove(self, src, dst, value):
        "Move ``value`` from set ``src`` to set ``dst`` atomically"
        return self.execute_command('SMOVE', src, dst, value)

    def spop(self, name):
        "Remove and return a random member of set ``name``"
        return self.execute_command('SPOP', name)

    def srandmember(self, name):
        "Return a random member of set ``name``"
        return self.execute_command('SRANDMEMBER', name)

    def srem(self, name, *values):
        "Remove ``value`` from set ``name``"
        return self.execute_command('SREM', name, *values)

    def sunion(self, keys, *args):
        "Return the union of sets specifiued by ``keys``"
        keys = list_or_args(keys, args)
        return self.execute_command('SUNION', *keys)

    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        keys = list_or_args(keys, args)
        return self.execute_command('SUNIONSTORE', dest, *keys)


    #### SORTED SET COMMANDS ####
    def zadd(self, name, *args):
        '''Add member the iterable over two dimensional elements ``args``.
The first element is the score and the second is the value.'''
        if len(args) % 2:
            raise RedisError(\
                    "ZADD requires an equal number of values and scores")
        return self.execute_command('ZADD', name, *args)

    def zcard(self, name):
        "Return the number of elements in the sorted set ``name``"
        return self.execute_command('ZCARD', name)

    def zincrby(self, name, value, amount=1):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        return self.execute_command('ZINCRBY', name, amount, value)

    def zrange(self, name, start, end, desc=False, withscores=False):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` indicates to sort in descending order.

        ``withscores`` indicates to return the scores along with the values.
            The return type is a list of (value, score) pairs
        """
        if desc:
            return self.zrevrange(name, start, end, withscores)
        pieces = ['ZRANGE', name, start, end]
        if withscores:
            pieces.append('withscores')
        return self.execute_command(*pieces, **{'withscores': withscores})

    def zrangebyscore(self, name, min, max,
            start=None, num=None, withscores=False):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the range.

        ``withscores`` indicates to return the scores along with the values.
            The return type is a list of (value, score) pairs
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")
        pieces = ['ZRANGEBYSCORE', name, min, max]
        if start is not None and num is not None:
            pieces.extend(['LIMIT', start, num])
        if withscores:
            pieces.append('withscores')
        return self.execute_command(*pieces, **{'withscores': withscores})

    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        return self.execute_command('ZRANK', name, value)

    def zrem(self, name, value):
        "Remove member ``value`` from sorted set ``name``"
        return self.execute_command('ZREM', name, value)

    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``
        """
        return self.execute_command('ZREMRANGEBYSCORE', name, min, max)

    def zrevrange(self, name, start, num, withscores=False):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``num`` sorted in descending order.

        ``start`` and ``num`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
            as a dictionary of value => score
        """
        pieces = ['ZREVRANGE', name, start, num]
        if withscores:
            pieces.append('withscores')
        return self.execute_command(*pieces, **{'withscores': withscores})

    def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return self.execute_command('ZREVRANK', name, value)

    def zscore(self, name, value):
        "Return the score of element ``value`` in sorted set ``name``"
        return self.execute_command('ZSCORE', name, value)

    def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate('ZINTERSTORE', dest, keys, aggregate)
    
    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate('ZUNIONSTORE', dest, keys, aggregate)
    
    def zdiffstore(self, dest, keys, aggregate=None, withscores=None):
        """
        Compute the difference of multiple sorted sets specified by
        ``keys`` into a new sorted set, ``dest``.
        """
        return self._zaggregate('ZDIFFSTORE', dest, keys,
                                aggregate = aggregate,
                                withscores = withscores)

    def _zaggregate(self, command, dest, keys,
                    aggregate=None, withscores = None):
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
            pieces.append(aggregate)
        return self.execute_command(*pieces)
    
    ### TIMESERIES COMMANDS ###
    def tslen(self, name):
        '''timeseries length'''
        return self.execute_command('TSLEN', name)
    
    def tsexists(self, name, score):
        '''timeseries length'''
        return self.execute_command('TSEXISTS', name, score)
    
    def tsrank(self, name, score):
        '''Rank of *score*'''
        return self.execute_command('TSRANK', name, score)

    def tsadd(self, name, *items):
        '''timeseries length'''
        return self.execute_command('TSADD', name, *items)
    
    def tsrange(self, name, start, end, desc = False,
                withtimes = False, novalues = False):
        """Return a range of values from timeseries at ``name`` between
``start`` and ``end`` sorted in ascending order.
``start`` and ``end`` can be negative, indicating the end of the range.

``desc`` indicates to sort in descending order.

``withscores`` indicates to return the scores along with the values.
    The return type is a list of (value, score) pairs"""
        pieces = ['TSRANGE', name, start, end]
        if withtimes:
            pieces.append('withtimes')
        elif novalues:
            pieces.append('novalues')
        return self.execute_command(*pieces, **{'withtimes': withtimes,
                                                'novalues': novalues})
        
    def tsrangebytime(self, key, start, end, desc = False,
                      withtimes = False, novalues = False):
        """Return a range of values from timeseries at ``key`` between
time ``start`` and time ``end`` sorted in ascending order.

``desc`` indicates to sort in descending order.

``withscores`` indicates to return the scores along with the values.
    The return type is a list of (value, score) pairs"""
        pieces = ['TSRANGEBYTIME', key, start, end]
        if withtimes:
            pieces.append('withtimes')
        elif novalues:
            pieces.append('novalues')
        return self.execute_command(*pieces, **{'withtimes': withtimes,
                                                'novalues': novalues})
    
    #### HASH COMMANDS ####
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

    # Scripting
    def eval(self, body, **kwargs):
        num_keys = len(kwargs)
        if num_keys:
            keys = []
            args = []
            for k,v in iteritems(kwargs):
                keys.append(k)
                args.append(v)
            keys.extend(args)
        else:
            keys = ()
        return self.execute_command('EVAL', body, num_keys, *keys)
    
    def script_call(self, name, *args, **options):
        '''Execute a registered lua script.'''
        script = get_script(name)
        if not script:
            raise ValueError('No such script {0}'.format(name))
        options['script_name'] = name
        options['script_args'] = args
        options['client'] = self
        return self.execute_command('EVAL',
                                    script.script, len(args), *args, **options)
    
    def delpattern(self, pattern):
        "delete all keys matching *pattern*."
        return self.script_call('delpattern', pattern)


class Pipeline(Redis):
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
    def __init__(self, connection_pool,  response_callbacks,
                 signal_on_send, signal_on_received):
        self.connection_pool = connection_pool
        self.response_callbacks = response_callbacks
        self.signal_on_send = signal_on_send
        self.signal_on_received = signal_on_received
        self.reset()

    def reset(self):
        self.command_stack = []
        self.execute_command('MULTI')
        
    @property
    def empty(self):
        return len(self.command_stack) <= 1

    def execute_command(self, *args, **options):
        """
Stage a command to be executed when execute() is next called

Returns the current Pipeline object back so commands can be
chained together, such as:

pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

At some other point, you can then run: pipe.execute(),
which will execute all commands queued in the pipe.
"""
        self.command_stack.append((args, options, []))
        return self
    
    def add_callback(self, callback):
        '''Adding a callback to the latest command in the pipeline.
Tipycal usage::

    pipe.sadd('foo').add_callback(mycallback)
    
The callback will be executed after the default callback for the command.
Several callbacks can be added.
'''
        if self.empty:
            raise ValueError('Cannot add callback. No command in the stack')
        self.command_stack[-1][2].append(callback)
        return self
                
    def parse_response(self, response, commands, **kwargs):
        return list(self._parse_response(response[-1], commands[1:-1]))
        
    def _parse_response(self, response, commands):
        self.signal_on_received.send(self,
                                     commands = commands,
                                     response = response)
        if len(response) != len(commands):
            raise ResponseError("Wrong number of response items from "
                "pipeline execution")
        parse_response = super(Pipeline,self).parse_response
        for r, cmd in zip(response, commands):
            if not isinstance(r, Exception):
                args, options, callbacks = cmd
                r = parse_response(r, args[0], **options)
                for callback in callbacks:
                    r = callback(r)
            yield r

    def execute(self):
        "Execute all the commands in the current pipeline"
        self.execute_command('EXEC')
        commands = self.command_stack
        self.reset()
        conn = self.connection_pool.get_connection()
        return conn.execute_pipeline(commands, self.parse_response)

