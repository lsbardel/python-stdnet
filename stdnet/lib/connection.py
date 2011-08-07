'''
This file was originally forked from redis-py in January 2011.
Since than it has moved on a different directions.

Copyright (c) 2010 Andy McCurdy
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
from .reader import RedisPythonReader


class PythonParser(object):

    def createReader(self, connection):
        return RedisPythonReader(connection)
    
    def on_connect(self, connection):
        self._sock = connection._sock
        self._reader = self.createReader(connection)

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


class HiredisParser(PythonParser):
    
    def createReader(self, connection):
        return hiredis.Reader(protocolError=InvalidResponse,
                              replyError=ResponseError)


try:
    import hiredis
    DefaultParser = HiredisParser
except ImportError:
    DefaultParser = PythonParser


class Connection(object):
    "Manages TCP communication to and from a Redis server"
    def __init__(self, host='localhost', port=6379, db=0, password=None,
                 socket_timeout=None, encoding='utf-8',
                 encoding_errors='strict', parser_class=None,
                 decode = False):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self._sock = None
        #if decode:
        #    self.decode = self._decode
        #else:
        #    self.decode = lambda x : x
        if parser_class is None:
            if settings.REDIS_PARSER == 'python':
                parser_class = PythonParser
            else:
                parser_class = DefaultParser
        self._parser = parser_class()

    def connect(self):
        "Connects to the Redis server if not already connected"
        if self._sock:
            return
        try:
            sock = self._connect()
        except socket.error as e:
            raise ConnectionError(self._error_message(e))

        self._sock = sock
        self.on_connect()

    def _connect(self):
        "Create a TCP socket connection"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)
        sock.connect((self.host, self.port))
        #sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        return sock

    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to %s:%s. %s." % \
                (self.host, self.port, exception.args[0])
        else:
            return "Error %s connecting %s:%s. %s." % \
                (exception.args[0], self.host, self.port, exception.args[1])

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

    def _send(self, command):
        "Send the command to the socket"
        if not self._sock:
            self.connect()
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

    def send_packed_command(self, command):
        "Send an already packed command to the Redis server"
        try:
            self._send(command)
        except ConnectionError:
            # retry the command once in case the socket connection simply
            # timed out
            self.disconnect()
            # if this _send() call fails, then the error will be raised
            self._send(command)

    def send_command(self, *args):
        "Pack and send a command to the Redis server"
        self.send_packed_command(self.pack_command(*args))

    def read_response(self):
        "Read the response from a previously sent command"
        response = self._parser.read_response()
        if response.__class__ == ResponseError:
            raise response
        return response
    
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
    

class ConnectionPool(object):
    "Generic connection pool"
    default_encoding = 'utf-8'
    
    def __init__(self,
                 connection_class=Connection,
                 max_connections=None,
                 **connection_kwargs):
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        if 'encoding' not in connection_kwargs:
            connection_kwargs['encoding'] = self.default_encoding
        self.max_connections = max_connections or settings.MAX_CONNECTIONS
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

    @property
    def db(self):
        return self.connection_kwargs['db']
    
    @property
    def encoding(self):
        return self.connection_kwargs['encoding']
    
    def get_connection(self, command_name, *keys, **options):
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
        return self.connection_class(**self.connection_kwargs)

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

