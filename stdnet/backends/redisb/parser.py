__all__ = ['RedisParser']

from collections import deque


REPLAY_TYPE = frozenset((b'$',  # REDIS_REPLY_STRING,
                         b'*',  # REDIS_REPLY_ARRAY,
                         b':',  # REDIS_REPLY_INTEGER,
                         b'+',  # REDIS_REPLY_STATUS,
                         b'-')) # REDIS_REPLY_ERROR

 
class StringTask(object):
    __slots__ = ('length',)
    
    def __init__(self, length):
        self.length = length
        
    def decode(self, parser):
        return parser.read(self.length)
    

class ArrayTask(object):
    __slots__ = ('length', '_response')
    
    def __init__(self, length):
        self.length = length
        self._response = []
        
    def decode(self, parser):
        while self.length > 0:
            response = parser._get_new()
            if response is None:
                break;
            self.length -= 1
            self._response.append(response)
        if not self.length:
            return self._response
                                     
    
class RedisParser(object):
    '''A python paraser for redis.'''

    def __init__(self, protocolError, responseError):
        self.protocolError = protocolError
        self.responseError = responseError
        self._stack = deque()
        self._inbuffer = bytearray()
    
    def on_connect(self, connection):
        pass

    def on_disconnect(self):
        pass
    
    def feed(self, buffer):
        '''Feed new data into the buffer'''
        self._inbuffer.extend(buffer)
        
    def get(self):
        '''Called by the Parser'''
        if self._stack:
            task = self._stack.popleft()
            result = task.decode(self)
            if result is None:
                self._stack.appendleft(task)
            return result
        else:
            return self._get_new()

    def buffer(self):
        '''Current buffer'''
        return bytes(self._inbuffer)
    
    def task(self, task):
        result = task.decode(self)
        if result is None:
            self._stack.append(task)
        return result
    
    def _get_new(self):
        response = self.read()
        if response is not None:
            rtype, response = bytes(response[:1]), bytes(response[1:])
            if rtype == b'-':
                return self.responseError(response)
            elif rtype == b':':
                return int(response)
            elif rtype == b'+':
                return response
            elif rtype == b'$':
                return self.task(StringTask(int(response)))
            elif rtype == b'*':
                return self.task(ArrayTask(int(response)))
            else:
                raise self.protocolError()

    def read(self, length=None):
        """
        Read a line from the buffer is no length is specified,
        otherwise read ``length`` bytes. Always strip away the newlines.
        """
        b = self._inbuffer
        if length is None:
            length = b.find(b'\r\n')
        if len(b) >= length+2:
            self._inbuffer, chunk = b[length+2:], b[:length]
            return chunk