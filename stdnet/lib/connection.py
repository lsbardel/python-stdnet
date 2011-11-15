'''
This file was originally forked from redis-py in January 2011.

Original Copyright
Copyright (c) 2010 Andy McCurdy
    BSD License

Since than it has moved on a different direction.

Copyright (c) 2011 Luca Sbardella
    BSD License

'''
import errno
import socket
import io
from itertools import chain, starmap

from stdnet.conf import settings
from stdnet.utils import to_bytestring, iteritems, map, ispy3k, range,\
                         to_string

from .exceptions import *
from .base import Reader, fallback


class RedisRequest(object):
    '''A redis request'''
    def __init__(self, connection, command_name, args,
                 parse_response = None, release_connection = True,
                 **options):
        self.connection = connection
        self.command_name = command_name
        self.args = args
        self.parse_response = parse_response
        self.release_connection = release_connection
        self.options = options
        self._response = None
        self.response = connection.parser.gets()
        if not self.command_name:
            self.response = []
            command = connection.pack_pipeline(args)
        else:
            command = connection.pack_command(command_name,*args)
        self.send(command)

    @property
    def num_responses(self):
        if self.command_name:
            return 1
        else:
            return len(self.args)
        
    @property
    def done(self):
        if self.command_name:
            return self.response is not False
        else:
            return len(self.response) == self.num_responses
                    
    def __str__(self):
        if self.command_name:
            return '{0}{1}'.format(self.command_name,self.args)
        else:
            return 'PIPELINE{0}'.format(self.args)
    
    def __repr__(self):
        return self.__str__()
        
    def _send(self, command):
        "Send the command to the socket"
        c = self.connection.connect()
        try:
            c.sock.sendall(command)
        except socket.error as e:
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
            self.connection.disconnect(release_connection = False)
            # if this _send() call fails, then the error will be raised
            self._send(command)
        
    def close(self):
        c = self.connection
        try:
            if isinstance(self.response,ResponseError):
                raise self.response
            if self.parse_response:
                self._response = self.parse_response(self.response,
                                            self.command_name or self.args,
                                            **self.options)
            else:
                self._response = self.response
        except:
            c.disconnect()
            raise
        if self.release_connection:
            c.pool.release(c)
        
    def parse(self, data):
        '''Got data from redis, feeds it to the :attr:`Connection.parser`.'''
        parser = self.connection.parser
        parser.feed(data)
        if self.command_name:
            self.response = parser.gets()
            if self.response is not False:
                self.close()
        else:
            while 1:
                response = parser.gets()
                if response is False:
                    break
                self.response.append(response)
            if len(self.response) == self.num_responses:
                self.close()
            
    def finish(self):
        raise NotImplementedError
        

class SyncRedisRequest(RedisRequest):
    '''A :class:`RedisRequest` for blocking sockets.'''
    def read_response(self):
        sock = self.connection.sock
        while not self.done:
            try:
                stream = sock.recv(io.DEFAULT_BUFFER_SIZE)
            except (socket.error, socket.timeout) as e:
                raise ConnectionError("Error while reading from socket: %s" % \
                    (e.args,))
            if not stream:
                raise ConnectionError("Socket closed on remote end")
            self.parse(stream)
        return self._response
    
    def finish(self):
        return self.read_response()
    
    
class Connection(object):
    ''''Manages TCP or UNIX communication to and from a Redis server.
This class should not be directly initialized. Insteady use the
:class:`ConnectionPool`::

    from stdnet.lib.connection ConnectionPool
    
    pool = ConnectionPool(('',6379),db=1)
    c = pool.get_connection()
    
.. attribute:: pool

    instance of the :class:`ConnectionPool` managing the connection
    
.. attribute:: parser

    instance of a Redis parser.
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
            if settings.REDIS_PARSER == 'python':
                reader_class = fallback.Reader
            else:
                reader_class = Reader
        self.parser = reader_class(InvalidResponse, ResponseError)

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
            
    def connect(self):
        "Connects to the Redis server if not already connected"
        if self.__sock:
            return self
        if self.socket_type == 'TCP':
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            #sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        elif self.socket_type == 'UNIX':
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.__sock = sock
        try:
            return self._connect()
        except socket.error as e:
            raise ConnectionError(self._error_message(e))

    def _connect(self):
        self.sock.settimeout(self.socket_timeout)
        self.sock.connect(self.address)
        return self.on_connect()
    
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
        # if a password is specified, authenticate
        OK = b'OK'
        if self.password:
            r = self.request('AUTH', self.password, release_connection = False)
            if r.read_response() != OK:
                raise ConnectionError('Invalid Password')

        # if a database is specified, switch to it
        if self.db:
            r = self.request('SELECT', self.db, release_connection = False)
            if r.read_response() != OK:
                raise ConnectionError('Invalid Database')
            
        return self

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
        pack = self.pack_command
        return b''.join(starmap(pack, (args for args,_ in commands)))
    
    def request(self, command_name, *args, **options):
        return self.request_class(self, command_name, args, **options)
        
    def execute_command(self, parse_response, command_name, *args, **options):
        options['parse_response'] = parse_response
        r = self.request(command_name, *args, **options)
        return r.finish()
    
    def execute_pipeline(self, parse_response, commands):
        r = self.request_class(self, None, commands,
                               parse_response = parse_response)
        return r.finish()


ConnectionClass = None


class ConnectionPool(object):
    "A :class:`Connection` pool."
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

