'''
This file was originally forked from redis-py in January 2011.
Since than it has moved on a different directions.

Original Copyright
Copyright (c) 2010 Andy McCurdy
    BSD License

Copyright (c) 2011 Luca Sbardella
    BSD License   

'''
import errno
import socket
from itertools import chain
from collections import namedtuple

from stdnet.conf import settings
from stdnet.utils import to_bytestring, iteritems, map, ispy3k, range,\
                         to_string

from .exceptions import *
from .base import Reader, fallback


class Parser(object):
    
    def __init__(self, rcls = None):
        self.rcls = rcls or Reader

    def createReader(self):
        return self.rcls(InvalidResponse, ResponseError)
    
    def on_connect(self, connection):
        self._sock = connection._sock
        self._reader = self.createReader()

    def on_disconnect(self):
        self._sock = None
        self._reader = None

    def read_response(self):
        response = self._reader.gets()
        while response is False:
            try:
                stream = self._sock.recv(4096)
            except (socket.error, socket.timeout) as e:
                raise ConnectionError("Error while reading from socket: %s" % \
                    (e.args,))
            if not stream:
                raise ConnectionError("Socket closed on remote end")
            self._reader.feed(stream)
            response = self._reader.gets()
        return response
        

class RedisRequest(object):
    '''A redis request'''
    def __init__(self, connection, command_name, args,
                 options, parse_response = None):
        self.connection = connection
        self.command_name = command_name
        self.parse_response = parse_response
        self.options = options
        command = connection.pack_command(command_name,*args)
        self.send(command)
        
    def _send(self, command):
        "Send the command to the socket"
        c = self.connection.connect()
        try:
            self._sock.sendall(command)
        except socket.error as e:
            if e.args[0] == errno.EPIPE:
                self.disconnect()
            if len(e.args) == 1:
                _errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                _errno, errmsg = e.args
            raise ConnectionError("Error %s while writing to socket. %s." % \
                (_errno, errmsg))

    def send(self, command):
        "Send an already packed command to the Redis server"
        try:
            self._send(command)
        except ConnectionError:
            # retry the command once in case the socket connection simply
            # timed out
            self.disconnect()
            # if this _send() call fails, then the error will be raised
            self._send(command)
        
    def close(self):
        c = self.connection
        try:
            "Read the response from a previously sent command"
            response = c._parser.read_response()
            if response.__class__ == ResponseError:
                raise response
            return self.parse_response(response, self.command_name,
                                       **self.options)
        except:
            c.disconnect()
            raise
        finally:
            self.connection_pool.release(c)


class Connection(object):
    ''''Manages TCP or UNIX communication to and from a Redis server.
This class should not be directly initialized. Insteady use the
:class:`ConnectionPool`::

    from stdnet.lib.connection ConnectionPool
    
    pool = ConnectionPool(('',6379),db=1)
    c = pool.get_connection()
    
.. attribute:: pool

    instance of the :class:`ConnectionPool` managing the connection
'''
    blocking = True
    request = RedisRequest
    
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
        self._sock = None
        if reader_class is None:
            if settings.REDIS_PARSER == 'python':
                reader_class = fallback.Reader
            else:
                reader_class = Reader
        self._parser = Parser(reader_class)

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
            
    def connect(self):
        "Connects to the Redis server if not already connected"
        if self._sock:
            return
        try:
            sock = self._connect()
        except socket.error as e:
            raise ConnectionError(self._error_message(e))

    def _connect(self):
        "Create a TCP socket connection"
        if self.socket_type == 'TCP':
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            #sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        elif self.socket_type == 'UNIX':
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            
        if self.blocking:
            sock.settimeout(self.socket_timeout)
            sock.connect(self.address)
            self._sock = sock
            self.on_connect()
        else:
            self.async_connect(sock)

    def async_connect(self, sock):
        raise NotImplementedError
    
    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to %s. %s." % \
                (self.address, exception.args[0])
        else:
            return "Error %s connecting %s. %s." % \
                (exception.args[0], self.address, exception.args[1])

    def on_connect(self):
        "Initialize the connection, authenticate and select a database"
        self._parser.on_connect(self)

        # if a password is specified, authenticate
        if self.password:
            self.send_command('AUTH', self.password)
            if self.read_response() != 'OK':
                raise ConnectionError('Invalid Password')

        # if a database is specified, switch to it
        if self.db:
            self.send_command('SELECT', self.db)
            if self.read_response() != OK:
                raise ConnectionError('Invalid Database')

    def disconnect(self):
        "Disconnects from the Redis server"
        self._parser.on_disconnect()
        if self._sock is None:
            return
        try:
            self._sock.close()
        except socket.error:
            pass
        self._sock = None

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
    
    def execute_command(self, parse_response, command_name, *args, **options):
        return self.request(self, command_name, args, options,
                            parse_response = parse_response)


ConnectionClass = None


class ConnectionPool(object):
    "Generic connection pool"
    default_encoding = 'utf-8'
    
    def __init__(self, address, connection_class=None, db=0,
                 max_connections=None, **connection_kwargs):
        self.__address = address
        self.__db = db
        self.connection_class = connection_class or\
                                settings.RedisConnectionClass or\
                                Connection
        self.connection_kwargs = connection_kwargs
        if 'encoding' not in connection_kwargs:
            connection_kwargs['encoding'] = self.default_encoding
        self.max_connections = max_connections or settings.MAX_CONNECTIONS
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

    def __hash__(self):
        return hash((self.address,self.db,self.connection_class))
    
    @property
    def address(self):
        return self.__address
    
    @property
    def db(self):
        return self.__db
    
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
            raise ConnectionError("Too many connections")
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

