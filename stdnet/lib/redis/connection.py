'''
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
from stdnet.lib import hiredis, fallback
from stdnet.utils.dispatch import Signal

from .exceptions import *


__all__ = ['RedisRequest',
           'ConnectionPool',
           'Connection',
           'RedisReader',
           'PyRedisReader',
           'redis_before_send',
           'redis_after_receive',
           'NOT_READY']


# SIGNALS
redis_before_send = Signal(providing_args=["request", "commands"])
redis_after_receive = Signal(providing_args=["request"])


PyRedisReader = lambda : fallback.RedisReader(RedisProtocolError,
                                              RedisInvalidResponse)
if hiredis:  #pragma    nocover
    RedisReader = lambda : hiredis.Reader(RedisProtocolError,
                                          RedisInvalidResponse)
else:   #pragma    nocover
    RedisReader = PyRedisReader
    

class NOT_READY:
    pass


class RedisRequest(BackendRequest):
    '''Redis request base class. A request instance manages the
handling of a single command from start to the response from the server.

.. attribute::    client

    A :class:`Redis` client or :class:`Pipeline`.
    
.. attribute::    connection

    the :class:`Connection` managing this request

.. attribute::    command_name

    The command name if :attr:`client` is a standard :class:`Redis` client,
    ``None`` if it is a :class:`Pipeline`
    
.. attribute::    release_connection

    if ``True`` this request will release the :attr:`connection` once it has
    obtained the response from the server.
    
.. attribute:: pooling

    The request is pooling data from redis server.
'''
    pooling = False
    
    def __init__(self, client, connection, command_name, args,
                 release_connection=True, **options):
        self.client = client
        self.connection = connection
        self.command_name = command_name
        self.args = args
        self.release_connection = release_connection
        self.options = options
        self._raw_response = []
        self._response = None
        self.response = connection.parser.gets()
        # if the command_name is missing, it means it is a pipeline of commands
        # in the args input parameter
        if command_name:
            self.command = connection.pack_command(command_name, *args)
        else:
            self.response = []
            self.command = connection.pack_pipeline(args)
            
    @property
    def num_responses(self):
        if self.command_name:
            return 1
        else:
            return len(self.args)
        
    @property
    def is_pipeline(self):
        return self.command_name is None
    
    @property
    def encoding(self):
        return self.client.encoding
    
    @property
    def raw_response(self):
        return b''.join(self._raw_response)
                    
    def __repr__(self):
        if self.command_name:
            return '{0}{1}'.format(self.command_name,self.args)
        else:
            return 'PIPELINE{0}'.format(self.args)
    __str__ = __repr__
        
    def close(self):
        redis_after_receive.send(self.client.__class__, request=self)
        c = self.connection
        try:
            response = self.client.parse_response(self)
            if isinstance(response, Exception):
                raise response
        except:
            c.disconnect()
            raise
        if self.release_connection:
            c.pool.release(c)
        return response
        
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
                return self.close()
        else:
            self.response = parser.gets()
            if self.response is not False:
                return self.close()
        return NOT_READY
        
    def disconnect(self):
        self.connection.disconnect()
        self.connection = None
        
    def send(self, result=None):
        "Send the command to the server"
        # broadcast BEFORE SEND signal
        redis_before_send.send(self.client.__class__,
                               request=self,
                               command=self.command)
        return self._write()
    
    def _write(self):
        return self.connection.sock.sendall(self.command)
    
    def execute(self):
        raise NotImplementedError()
    
    def pool(self, num_messages=None):
        raise NotImplementedError()


class SyncRedisRequest(RedisRequest):
    '''A :class:`RedisRequest` for blocking sockets.'''
    retry = 2
    def execute(self, tried=0):
        tried += 1
        self.tried = tried
        try:
            try:
                self.connection.connect(self)
                self.send()
                return self.read_response()
            except (socket.timeout, socket.error):
                if tried < self.retry:
                    self.connection.disconnect(release_connection=False)
                    return self.execute(tried)
                else:
                    raise
        except:
            self.disconnect()
            raise
        return self.read_response()
    
    def read_response(self):
        '''Read a redis response from the socket and parse it.'''
        response = NOT_READY
        sock = self.connection.sock
        while response is NOT_READY:
            stream = sock.recv(io.DEFAULT_BUFFER_SIZE)
            response = self.parse(stream)
        return response
    
    def pool(self, num_messages=None):
        if not self.pooling:
            self.pooling = True
            count = 0
            while self.pooling:
                try:
                    yield self.read_response()
                except:
                    self.pooling = False
                    raise
                count += 1
                if num_messages and count >= num_messages:
                    break 
            self.pooling = False
    
    
class Connection(object):
    ''''Manages TCP or UNIX communication to and from a Redis server.
This class should not be directly initialized. Instead use the
:class:`ConnectionPool`::

    from stdnet.lib.connection ConnectionPool
    
    pool = ConnectionPool(('',6379), db=1)
    c = pool.get_connection()
    
.. attribute:: pool

    instance of the :class:`ConnectionPool` managing the connection
    
.. attribute:: parser

    instance of a Redis parser.
    
.. attribute:: sock

    Python socket which handle the sending and receiving of data.
'''
    request_class = SyncRedisRequest
    socket_class = socket.socket
    encoding_errors = 'strict'
    
    "Manages TCP communication to and from a Redis server"
    def __init__(self, pool, reader_class=None):
        self.pool = pool
        self._sock = None
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
    def encoding(self):
        '''Redis server address'''
        return self.pool.encoding
    
    @property
    def db(self):
        '''Redis server database number'''
        return self.pool.db

    @property
    def password(self):
        '''Redis server database number'''
        return self.pool.password
        
    @property
    def socket_timeout(self):
        return self.pool.socket_timeout
    
    @property
    def socket_type(self):
        '''Socket type'''
        if isinstance(self.address, tuple):
            return 'TCP'
        elif isinstance(self.address, str):
            if os.name == 'posix':
                return 'UNIX'
            else:
                raise ValueError('Unix socket available on posix systems only')
    
    @property
    def sock(self):
        '''Connection socket'''
        return self._sock

    @property
    def WRITE_BUFFER_SIZE(self):
        return self.pool.WRITE_BUFFER_SIZE
    
    @property
    def READ_BUFFER_SIZE(self):
        return self.pool.READ_BUFFER_SIZE
    
    def connect(self, request):
        "Connects to the Redis server if not already connected."
        if self._sock:
            return
        if self.socket_type == 'TCP':
            sock = self.socket_class(socket.AF_INET, socket.SOCK_STREAM)
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
            sock = self.socket_class(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)
        try:
            sock.connect(self.address)
        except socket.error as e:
            raise RedisConnectionError(self._error_message(e))
        self._sock = self._wrap_socket(sock)
        return self.on_connect(request)

    #    INTERNAL METHODS
    
    def _wrap_socket(self, sock):
        return sock
    
    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to %s. %s." % \
                (self.address, exception.args[0])
        else:
            return "Error %s connecting %s. %s." % \
                (exception.args[0], self.address, exception.args[1])

    def on_connect(self, request):
        "Initialize the connection, authenticate and select a database"
        # if a password is specified, authenticate
        client = request.client.client
        if self.password:
            r = self.request(client, 'AUTH', self.password,
                             release_connection=False).execute()
            if not r:
                raise RedisConnectionError('Invalid Password')
        # if a database is specified, switch to it
        if self.db:
            r = self.request(client, 'SELECT', self.db,
                             release_connection=False).execute()
            if not r:
                raise RedisConnectionError('Invalid Database "%s"' % self.db)
        return request

    def disconnect(self, release_connection=True):
        "Disconnects from the Redis server"
        if self._sock is not None:
            try:
                self._sock.close()
            except socket.error:
                pass
            self._sock = None
        if release_connection:
            self.pool.release(self)

    if ispy3k:
        def encode(self, value):
            return value if isinstance(value, bytes) else str(value).encode(
                                        self.encoding,self.encoding_errors)
            
    else:   #pragma    nocover
        def encode(self, value):
            if isinstance(value,unicode):
                return value.encode(self.encoding, self.encoding_errors)
            else:
                return str(value)
    
    def __pack_gen(self, args):
        e = self.encode
        crlf = b'\r\n'
        yield e('*%s\r\n'%len(args))
        for value in map(e, args):
            yield e('$%s\r\n'%len(value))
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
        
    def request(self, client, command_name, *args, **options):
        return self.request_class(client, self, command_name, args, **options)

ConnectionClass = None


class ConnectionPool(object):
    "A :class:`Redis` :class:`Connection` pool."
    connection_pools = {}
    WRITE_BUFFER_SIZE = 128 * 1024
    READ_BUFFER_SIZE = io.DEFAULT_BUFFER_SIZE
    encoding = 'utf-8'
    
    @classmethod
    def create(cls, address=None, connection_class=None, db=0,
               max_connections=None, encoding=None,
               socket_timeout=None, password=None):
        if not address:
            raise ValueError('Redis connection address not supplied')
        o = ConnectionPool()
        o._address = address
        o._db = db
        o.password = password
        o.connection_class = connection_class or\
                             settings.RedisConnectionClass or Connection
        o.encoding = encoding or cls.encoding
        o.socket_timeout = socket_timeout
        if o not in cls.connection_pools:
            o.max_connections = max_connections or settings.MAX_CONNECTIONS
            o._init()
            cls.connection_pools[o] = o
        return cls.connection_pools[o]
        
    def _init(self):
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

    def __hash__(self):
        return hash((self.address, self.db, self.connection_class,
                     self.socket_timeout, self.password))
    
    @property
    def address(self):
        return self._address
    
    @property
    def db(self):
        return self._db
    
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
        return self.connection_class(self)

    def release(self, connection):
        "Releases the connection back to the pool"
        self._in_use_connections.discard(connection)
        if connection not in self._available_connections: 
            self._available_connections.append(connection)

    def disconnect(self):
        "Disconnects all connections in the pool"
        for connection in list(chain(self._available_connections,
                                     self._in_use_connections)):
            connection.disconnect()

    def clone(self, **kwargs):
        c = copy(self)
        c._init()
        for k,v in kwargs.items():
            if k in ('address','db'):
                k = '_' + k
            setattr(c, k, v)
        if c not in self.connection_pools:
            self.connection_pools[c] = c
        return self.connection_pools[c]
        