'''
This file was originally forked from redis-py in January 2011.

Original Copyright
Copyright (c) 2010 Andy McCurdy
    BSD License

Since than it has moved on a different direction.

Copyright (c) 2011 Luca Sbardella
    BSD License

A note on TCP performance.

TCP depends on several factors for performance. Two of the most important

* the link bandwidth, or LB, the rate at which packets can be transmitted
  on the network
* the round-trip time, or RTT, the delay between a segment being sent and
  its acknowledgment from the peer.
  
These two values determine what is called the Bandwidth Delay Product (BDP)::

    BDP = LB * RTT
    

If your application communicates over a 100Mbps local area network with a
50 ms RTT, the BDP is::

    100MBps * 0.050 sec / 8 = 0.625MB = 625KB
    
I divide by 8 to convert from bits to bytes communicated.

The BDP is the theoretical optimal TCP socket buffer size,  If the buffer is
too small, the TCP window cannot fully open, and this limits performance.
If it's too large, precious memory resources can be wasted.
If you set the buffer just right, you can fully utilize the available
bandwidth.

In the example above the optimal buffer size is 625KB.
To obtain the buffer sizes used by the socket you can use
    
    import socket
    s = socket.socket()
    s.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
    s.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
'''
import os
import errno
import socket
import io
from copy import copy
from itertools import chain, starmap

from stdnet import BackendRequest
from stdnet.conf import settings
from stdnet.utils import iteritems, map, ispy3k, range, to_string
from stdnet.lib import hr, fallback
from stdnet.utils.dispatch import Signal

from .exceptions import *


__all__ = ['RedisRequest',
           'ConnectionPool',
           'Connection',
           'RedisReader',
           'PyRedisReader',
           'redis_before_send',
           'redis_after_receive']


# SIGNALS
redis_before_send = Signal(providing_args=["request", "commands"])
redis_after_receive = Signal(providing_args=["request"])


PyRedisReader = lambda : fallback.RedisReader(RedisProtocolError,
                                              RedisInvalidResponse)
if hr:
    RedisReader = lambda : hr.RedisReader(RedisProtocolError,
                                          RedisInvalidResponse)
else:
    RedisReader = PyRedisReader
    


class RedisRequest(BackendRequest):
    '''Redis request base class. A request instance manages the
handling of a single command from start to the response from the server.'''
    retry = 2
    def __init__(self, client, connection, command_name, args,
                 release_connection = True, **options):
        self.client = client
        self.connection = connection
        self.command_name = command_name
        self.args = args
        self.release_connection = release_connection
        self.options = options
        self.tried = 0
        self._raw_response = []
        self._response = None
        self._is_pipeline = False
        self.response = connection.parser.gets()
        # if the command_name is missing, it means it is a pipeline of commands
        # in the args input parameter
        if self.command_name is None:
            self._is_pipeline = True
            self.response = []
            self.command = connection.pack_pipeline(args)
        elif self.command_name:
            self.command = connection.pack_command(command_name, *args)
        else:
            self.command = None

    @property
    def num_responses(self):
        if self.command_name:
            return 1
        else:
            return len(self.args)
        
    @property
    def is_pipeline(self):
        return self._is_pipeline
    
    @property
    def encoding(self):
        return self.client.encoding
    
    @property
    def done(self):
        if self.is_pipeline:
            return len(self.response) == self.num_responses
        else:
            return self.response is not False
        
    @property
    def raw_response(self):
        return b''.join(self._raw_response)
                    
    def __str__(self):
        if self.command_name:
            return '{0}{1}'.format(self.command_name,self.args)
        else:
            return 'PIPELINE{0}'.format(self.args)
    
    def __repr__(self):
        return self.__str__()
        
    def _send(self):
        "Send the command to the server"
        # broadcast BEFORE SEND signal
        redis_before_send.send(self.client.__class__,
                               request = self,
                               command = self.command)
        self.connection.connect(self, self.tried)
        try:
            self.connection.sock.sendall(self.command)
        except socket.error as e:
            if len(e.args) == 1:
                _errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                _errno, errmsg = e.args
            raise RedisConnectionError("Error %s while writing to socket. %s." % \
                (_errno, errmsg))
        
    def close(self):
        redis_after_receive.send(self.client.__class__, request=self)
        c = self.connection
        try:
            #if isinstance(self.response, ResponseError):
            #    if str(self.response) == NoScriptError.msg:
            #        self.response = NoScriptError()
            #    else:
            #        raise self.response
            self._response = self.client.parse_response(self)
            if isinstance(self._response, Exception):
                raise self._response
        except:
            c.disconnect()
            raise
        if self.release_connection:
            c.pool.release(c)
        
    def parse(self, data):
        '''Got data from redis, feeds it to the :attr:`Connection.parser`.'''
        self._raw_response.append(data)
        parser = self.connection.parser
        parser.feed(data)
        if self.is_pipeline:
            while 1:
                response = parser.gets()
                if response is False:
                    break
                self.response.append(response)
            if len(self.response) == self.num_responses:
                self.close()
        else:
            self.response = parser.gets()
            if self.response is not False:
                self.close()
        
    def execute(self):
        raise NotImplementedError()
    

class SyncRedisRequest(RedisRequest):
    '''A :class:`RedisRequest` for blocking sockets.'''
    def execute(self):
        self.tried = 1
        while self.tried < self.retry:
            try:
                return self._sendrecv()
            except RedisConnectionError as e:
                if e.retry:
                    self.connection.disconnect(release_connection = False)
                    self.tried += 1
                else:
                    raise
        return self._sendrecv()
    
    def _sendrecv(self):
        self._send()
        return self.read_response()
    
    def read_response(self):
        sock = self.connection.sock
        while not self.done:
            try:
                stream = sock.recv(io.DEFAULT_BUFFER_SIZE)
            except (socket.error, socket.timeout) as e:
                raise RedisConnectionError("Error while reading from socket: %s" % \
                        (e.args,))
            if not stream:
                raise RedisConnectionError("Socket closed on remote end", True)
            self.parse(stream)
        return self._response
    
    
class Connection(object):
    ''''Manages TCP or UNIX communication to and from a Redis server.
This class should not be directly initialized. Instead use the
:class:`ConnectionPool`::

    from stdnet.lib.connection ConnectionPool
    
    pool = ConnectionPool(('',6379),db=1)
    c = pool.get_connection()
    
.. attribute:: pool

    instance of the :class:`ConnectionPool` managing the connection
    
.. attribute:: parser

    instance of a Redis parser.
    
.. attribute:: sock

    Python socket which handle the sending and receiving of data.
'''
    request_class = SyncRedisRequest
    
    "Manages TCP communication to and from a Redis server"
    def __init__(self, pool, password=None,
                 socket_timeout=None, encoding='utf-8',
                 encoding_errors='strict', reader_class=None,
                 decode = False, **kwargs):
        self.pool = pool
        self.password = password
        self.socket_timeout = socket_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.__sock = None
        if reader_class is None:
            if settings.REDIS_PY_PARSER:
                reader_class = PyRedisReader
            else:
                reader_class = RedisReader
        self.parser = reader_class()

    @property
    def address(self):
        '''Redis server address'''
        return self.pool.address
    
    @property
    def db(self):
        '''Redis server database number'''
        return self.pool.db
    
    @property
    def socket_type(self):
        '''Socket type'''
        if isinstance(self.address,tuple):
            return 'TCP'
        elif isinstance(self.address,str):
            if os.name == 'posix':
                return 'UNIX'
            else:
                raise ValueError('Unix socket available on posix systems only')
    
    @property
    def sock(self):
        '''Connection socket'''
        return self.__sock

    @property
    def WRITE_BUFFER_SIZE(self):
        return self.pool.WRITE_BUFFER_SIZE
    
    @property
    def READ_BUFFER_SIZE(self):
        return self.pool.READ_BUFFER_SIZE
    
    def connect(self, request, counter = 1):
        "Connects to the Redis server if not already connected."
        if self.__sock:
            return
        if self.socket_type == 'TCP':
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Disables Nagle's algorithm so that we send the data we mean and
            # better speed.
            # (check http://en.wikipedia.org/wiki/Nagle%27s_algorithm)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # set the socket buffer size
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF,
                            self.WRITE_BUFFER_SIZE)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                            self.READ_BUFFER_SIZE)
        elif self.socket_type == 'UNIX':
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.__sock = sock
        try:
            return self._connect(request, counter)
        except socket.error as e:
            raise RedisConnectionError(self._error_message(e))

    def _connect(self, request, counter):
        self.sock.settimeout(self.socket_timeout)
        self.sock.connect(self.address)
        return self.on_connect(request, counter)
    
    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to %s. %s." % \
                (self.address, exception.args[0])
        else:
            return "Error %s connecting %s. %s." % \
                (exception.args[0], self.address, exception.args[1])

    def on_connect(self, request, counter):
        "Initialize the connection, authenticate and select a database"
        # if a password is specified, authenticate
        client = request.client.client
        if self.password:
            r = self.execute_command(client, 'AUTH', self.password,
                                     release_connection = False)
            if not r:
                raise RedisConnectionError('Invalid Password ({0})'.format(counter))

        # if a database is specified, switch to it
        if self.db:
            r = self.execute_command(client, 'SELECT', self.db,
                                     release_connection = False)
            if not r:
                raise RedisConnectionError('Invalid Database "{0}". ({1})'\
                                      .format(self.db, counter))
            
        return request

    def disconnect(self, release_connection = True):
        "Disconnects from the Redis server"
        if self.__sock is None:
            return
        try:
            self.__sock.close()
        except socket.error:
            pass
        self.__sock = None
        if release_connection:
            self.pool.release(self)

    if ispy3k:
        def encode(self, value):
            return value if isinstance(value,bytes) else str(value).encode(
                                        self.encoding,self.encoding_errors)
            
    else:
        def encode(self, value):
            if isinstance(value,unicode):
                return value.encode(self.encoding,self.encoding_errors)
            else:
                return str(value)
    
    def _decode(self, value):
        return value.decode(self.encoding,self.encoding_errors)
    
    def __pack_gen(self, args):
        crlf = b'\r\n'
        yield b'*'
        yield str(len(args)).encode(self.encoding)
        yield crlf
        for value in map(self.encode,args):
            yield b'$'
            yield str(len(value)).encode(self.encoding)
            yield crlf
            yield value
            yield crlf
    
    def pack_command(self, *args):
        "Pack a series of arguments into a value Redis command"
        return b''.join(self.__pack_gen(args))
    
    def pack_pipeline(self, commands):
        '''Internal function for packing pipeline commands into a
command byte to be send to redis.'''
        pack = self.pack_command
        return b''.join(starmap(pack, ((c.command,)+c.args for c in commands)))
        
    def execute_command(self, client, command_name, *args, **options):
        return self.request_class(client, self, command_name, args, **options)\
                   .execute()
    
    def execute_pipeline(self, client, commands):
        '''Execute a :class:`Pipeline` in the server.

:parameter commands: the list of commands to execute in the server.
:parameter parse_response: callback for parsing the response from server.
:rtype: ?'''
        return self.request_class(client, self, None, commands).execute()

ConnectionClass = None


class ConnectionPool(object):
    "A :class:`Redis` :class:`Connection` pool."
    default_encoding = 'utf-8'
    
    def __init__(self, address, connection_class = None, db = 0,
                 max_connections=None, **connection_kwargs):
        if not address:
            raise ValueError('Redis connection address not supplied')
        self._address = address
        self._db = db
        self.connection_class = connection_class or\
                                settings.RedisConnectionClass or\
                                Connection
        self.connection_kwargs = connection_kwargs
        if 'encoding' not in connection_kwargs:
            connection_kwargs['encoding'] = self.default_encoding
        self.max_connections = max_connections or settings.MAX_CONNECTIONS
        self.WRITE_BUFFER_SIZE = 128 * 1024
        self.READ_BUFFER_SIZE = io.DEFAULT_BUFFER_SIZE
        self._init()
        
    def _init(self):
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

    def __hash__(self):
        return hash((self.address,self.db,self.connection_class))
    
    @property
    def address(self):
        return self._address
    
    @property
    def db(self):
        return self._db
    
    @property
    def encoding(self):
        return self.connection_kwargs['encoding']
    
    def __eq__(self, other):
        if isinstance(other,self.__class__):
            return self.address == other.address and self.db == other.db
        else:
            return False
    
    def get_connection(self):
        "Get a connection from the pool"
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = self.make_connection()
        self._in_use_connections.add(connection)
        return connection

    def make_connection(self):
        "Create a new connection"
        if self._created_connections >= self.max_connections:
            raise RedisConnectionError("Too many connections")
        self._created_connections += 1
        return self.connection_class(self, **self.connection_kwargs)

    def release(self, connection):
        "Releases the connection back to the pool"
        self._in_use_connections.remove(connection)
        self._available_connections.append(connection)

    def disconnect(self):
        "Disconnects all connections in the pool"
        all_conns = chain(self._available_connections,
                          self._in_use_connections)
        for connection in all_conns:
            connection.disconnect()

    def clone(self, **kwargs):
        c = copy(self)
        c._init()
        for k,v in kwargs.items():
            if k in ('address','db'):
                k = '_'+k
            setattr(c, k, v)
        return c
        