from stdnet.utils import BytesIO

from .exceptions import *


REDIS_REPLY_STRING = 1
REDIS_REPLY_ARRAY = 2
REDIS_REPLY_INTEGER = 3
REDIS_REPLY_NIL = 4
REDIS_REPLY_STATUS = 5
REDIS_REPLY_ERROR = 6
REDIS_ERR = 7

 
class redisReadTask(object):
    __slots__ = ('type','response','length','connection')
    REPLAY_TYPE = {b'$':REDIS_REPLY_STRING,
                   b'*':REDIS_REPLY_ARRAY,
                   b':':REDIS_REPLY_INTEGER,
                   b'+':REDIS_REPLY_STATUS,
                   b'-':REDIS_REPLY_ERROR}
    
    def __init__(self, type, response, connection):
        self.connection = connection
        if type in self.REPLAY_TYPE:
            self.type = rtype = self.REPLAY_TYPE[type]
            length = None
            if rtype == REDIS_REPLY_ERROR:
                if response.startswith(ERR):
                    response = ResponseError(response[4:])
                elif response.startswith(LOADING):
                    raise ConnectionError("Redis is loading data into memory")
            elif rtype == REDIS_REPLY_INTEGER:
                response = int(response)
            elif rtype == REDIS_REPLY_STRING:
                length = int(response)
                response = b''
            elif rtype == REDIS_REPLY_ARRAY:
                length = int(response)
                response = []
            #elif rtype == REDIS_REPLY_STATUS:
            #    response = self.connection.decode(response)
            self.response = response
            self.length = length
        else:
            raise InvalidResponse('Protocol Error.\
 Could not decode type "{0}"'.format(type)) 
        
    def gets(self, response = False, recursive = False):
        gets = self.connection.gets
        read = self.connection.read
        stack = self.connection._stack
        if self.type == REDIS_REPLY_STRING:
            if response is False:
                if self.length == -1:
                    return None
                response = read(self.length)
                if response is False:
                    stack.append(self)
                    return False
            #self.response = self.connection.decode(response)
            self.response = response
        elif self.type == REDIS_REPLY_ARRAY:
            length = self.length
            if length == -1:
                return None
            stack.append(self)
            append = self.response.append
            if response is not False:
                length -= 1
                append(response)
            while length > 0:
                response = gets(True)
                if response is False:
                    self.length = length
                    return False
                length -= 1
                append(response)
            stack.pop()
        
        if stack and not recursive:
            task = stack.pop()
            return task.gets(self.response,recursive)
        
        return self.response
                         

class RedisPythonReaderLegacy(object):
    '''The version below is slightly faster'''
    def __init__(self, connection):
        self._stack = []
        self._inbuffer = bytearray()

    def read(self, length=None):
        """Read a line from the socket is length is None,
otherwise read ``length`` bytes"""
        chunk = None
        try:
            if length is not None:
                length += 2
                if len(self._inbuffer) >= length:
                    chunk = bytes(self._inbuffer[:length])
            else:
                length = self._inbuffer.find(b'\n') + 1
                if length > 0:
                    chunk = bytes(self._inbuffer[0:length])
            if chunk:
                del self._inbuffer[0:len(chunk)]
                return chunk[:-2]
            return False
        except socket.error as e:
            if e.args and e.args[0] == errno.EAGAIN:
                raise ConnectionError("Error while reading from socket: %s" % \
                    e.args[1])
    
    def feed(self, buffer):
        self._inbuffer.extend(buffer)
        
    def gets(self, recursive = False):
        '''Called by the Parser'''
        if self._stack and not recursive:
            task = self._stack.pop()
        else:
            response = self.read()
            if not response:
                return False
            task = redisReadTask(response[:1], response[1:], self)
        return task.gets(recursive=recursive)
    
    
class RedisPythonReader(object):

    def __init__(self, connection):
        self._stack = []
        self._inbuffer = BytesIO()
        #self.decode = connection.decode
    
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
                if chunk[-2:] == b'\r\n':
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
        if self._stack and not recursive:
            task = self._stack.pop()
        else:
            response = self.read()
            if not response:
                return False
            task = redisReadTask(response[:1], response[1:], self)
        return task.gets(recursive=recursive)
