__all__ = ['RedisParser']

from stdnet.utils import long

REPLAY_TYPE = frozenset((b'$',  # REDIS_REPLY_STRING,
                         b'*',  # REDIS_REPLY_ARRAY,
                         b':',  # REDIS_REPLY_INTEGER,
                         b'+',  # REDIS_REPLY_STATUS,
                         b'-')) # REDIS_REPLY_ERROR

 
class String(object):
    __slots__ = ('_length', 'next')
    
    def __init__(self, length, next):
        self._length = length
        self.next = next
        
    def decode(self, parser, result):
        parser._current = None
        length = self._length
        if length >= 0:
            b = parser._inbuffer
            if len(b) >= length+2:
                parser._inbuffer, chunk = b[length+2:], bytes(b[:length])
                if parser.encoding:
                    return chunk.decode(parser.encoding)
                else:
                    return chunk
            else:
                parser._current = self
                return False
            

class ArrayTask(object):
    __slots__ = ('_length', '_response', 'next')
    
    def __init__(self, length, next):
        self._length = length
        self._response = []
        self.next = next
        
    def decode(self, parser, result):
        length = self._length
        if length >= 0:
            response = self._response
            if result is not False:
                response.append(result)
            while len(response) < length:
                result = parser._get(self)
                if result is False:
                    break
                response.append(result)
            if len(response) == length:
                parser._current = None
                return response
            elif not parser._current:
                parser._current = self
            return False
    
    
class RedisParser(object):
    '''A python paraser for redis.'''
    encoding = None
    
    def __init__(self, protocolError, responseError):
        self.protocolError = protocolError
        self.responseError = responseError
        self._current = None
        self._inbuffer = bytearray()
    
    def on_connect(self, connection):
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self):
        pass
    
    def feed(self, buffer):
        '''Feed new data into the buffer'''
        self._inbuffer.extend(buffer)
        
    def get(self):
        '''Called by the Parser'''
        if self._current:
            return self._resume(self._current, False)
        else:
            return self._get(None)
    
    def _get(self, next):
        b = self._inbuffer
        length = b.find(b'\r\n')
        if length >= 0:
            self._inbuffer, response = b[length+2:], bytes(b[:length])
            rtype, response = response[:1], response[1:]
            if rtype == b'-':
                return self.responseError(response.decode('utf-8'))
            elif rtype == b':':
                return long(response)
            elif rtype == b'+':
                return response
            elif rtype == b'$':
                task = String(long(response), next)
                return task.decode(self, False)
            elif rtype == b'*':
                task = ArrayTask(long(response), next)
                return task.decode(self, False)
            else:
                # Clear the buffer and raise
                self._inbuffer = bytearray()
                raise self.protocolError('Protocol Error')
        else:
            return False
                
    def buffer(self):
        '''Current buffer'''
        return bytes(self._inbuffer)
    
    def _resume(self, task, result):
        result = task.decode(self, result)
        if result is not False and task.next:
            return self._resume(task.next, result)
        else:
            return result