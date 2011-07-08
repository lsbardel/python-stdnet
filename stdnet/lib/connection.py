'''
This file was originally forked from redis-py in January 2011.
Since than it has moved on a different directions.

Copyright (c) 2010 Andy McCurdy
    BSD License   


'''
import errno
import socket
from itertools import chain
from collections import namedtuple

from stdnet.conf import settings
from stdnet.utils import to_bytestring, iteritems, map, ispy3k, range,\
                         to_string, BytesIO

if ispy3k:
    toint = lambda x : int(x)
else:
    toint = lambda x : long(x)

from .exceptions import *


REDIS_REPLY_STRING = 1
REDIS_REPLY_ARRAY = 2
REDIS_REPLY_INTEGER = 3
REDIS_REPLY_NIL = 4
REDIS_REPLY_STATUS = 5
REDIS_REPLY_ERROR = 6
REDIS_ERR = 7


REPLAY_TYPE = {b'$':REDIS_REPLY_STRING,
               b'*':REDIS_REPLY_ARRAY,
               b':':REDIS_REPLY_INTEGER,
               b'+':REDIS_REPLY_STATUS,
               b'-':REDIS_REPLY_ERROR}
       
 
class redisReadTask(object):
    __slots__ = ('type','response','length')
    
    def __init__(self, type, response):
        self.type = rtype = REPLAY_TYPE.get(type,REDIS_ERR)
        length = None
        if rtype == REDIS_REPLY_ERROR:
            if response.startswith(ERR):
                response = ResponseError(response[4:])
            elif response.startswith(LOADING):
                raise ConnectionError("Redis is loading data into memory")
        elif rtype == REDIS_REPLY_INTEGER:
            response = toint(response)
        elif rtype == REDIS_REPLY_STRING:
            length = toint(response)
            response = b''
        elif rtype == REDIS_REPLY_ARRAY:
            length = toint(response)
            response = []
        elif rtype == REDIS_ERR:
            raise InvalidResponse("Protocol Error")        
        self.response = response
        self.length = length
        

class RedisPythonReader(object):

    def __init__(self, connection):
        self._stack = []
        self._inbuffer = BytesIO()
        self.encoding = connection.encoding
        self.encoding_errors = connection.encoding_errors
            
    def read(self, length = None):
        """
        Read a line from the socket is no length is specified,
        otherwise read ``length`` bytes. Always strip away the newlines.
        """
        try:
            if length is not None:
                chunk = self._inbuffer.read(length+2)
            else:
                chunk = self._inbuffer.readline()
            if chunk:
                if chunk[-1:] == b'\n':
                    return chunk[:-2]
                else:
                    self._inbuffer = BytesIO(chunk)
            return False
        except (socket.error, socket.timeout) as e:
            raise ConnectionError("Error while reading from socket: %s" % \
                (e.args,))
    
    def feed(self, buffer):
        buffer = self._inbuffer.read(-1) + buffer
        self._inbuffer = BytesIO(buffer)
        
    def gets(self, recursive = False):
        '''Called by the Parser'''
        response = self.read()
        if not response:
            return False
        
        if self._stack and self._stack[-1].type == REDIS_REPLY_STRING:
            task = self._stack.pop()
        else:
            task = redisReadTask(response[:1], response[1:])

        # server returned an error
        rtype = task.type
        if rtype == REDIS_REPLY_STRING:
            if task.length == -1:
                return None
            response = self.read(task.length)
            if response is False:
                self._stack.append(task)
                return False
            task.response = response
        elif rtype == REDIS_REPLY_ARRAY:
            if task.length == -1:
                return None
            length = task.length
            self._stack.append(task)
            read = self.gets
            append = task.response.append
            while length > 0:
                response = read(True)
                if response is False:
                    task.length = length
                    return False
                length -= 1
                append(response)
            task = self._stack.pop()
        
        if self._stack and not recursive:
            return self.gets(True)
        return task.response


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
                buffer = self._sock.recv(4096)
            except (socket.error, socket.timeout) as e:
                raise ConnectionError("Error while reading from socket: %s" % \
                    (e.args,))
            if not buffer:
                raise ConnectionError("Socket closed on remote end")
            self._reader.feed(buffer)
            # if the data received doesn't end with \r\n, then there's more in
            # the socket
            if not buffer.endswith(CRLF):
                continue
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
                 encoding_errors='strict', parser_class=None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self._sock = None
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

    def encode(self, value):
        "Return a bytestring representation of the value"
        return to_bytestring(value, self.encoding, self.encoding_errors)

    def pack_command(self, *args):
        "Pack a series of arguments into a value Redis command"
        crlf = b'\r\n'
        chunk = BytesIO()
        enc = self.encoding
        err = self.encoding_errors
        write = chunk.write
        for value in map(self.encode, args):
            write(b'$')
            write(str(len(value)).encode(enc,err))
            write(crlf)
            write(value)
            write(crlf)
        data = BytesIO()
        write = data.write
        write(b'*')
        write(str(len(args)).encode(enc,err))
        write(crlf)
        write(chunk.getvalue())
        return data.getvalue()
        

class ConnectionPool(object):
    "Generic connection pool"
    def __init__(self,
                 connection_class=Connection,
                 max_connections=None,
                 **connection_kwargs):
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections or settings.MAX_CONNECTIONS
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

    @property
    def db(self):
        return self.connection_kwargs['db']
    
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

