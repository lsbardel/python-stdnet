__all__ = ['RedisParser']


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
        
    def decode(self, parser):
        length = self._length
        if length >= 0:
            b = parser._inbuffer
            if len(b) >= length+2:
                parser._current = None
                parser._inbuffer, chunk = b[length+2:], bytes(b[:length])
                if parser.encoding:
                    return chunk.decode(parser.encoding)
                else:
                    return chunk
            else:
                parser._current = self
                return False
    
    def resume(self, parser):
        result = self.decode(parser)
        if result is not False and self.next:
            return self.next.resume(parser, result)
        return result
            

class ArrayTask(object):
    __slots__ = ('_left', '_response', 'next')
    
    def __init__(self, length, next):
        self._left = length
        self._response = []
        self.next = next
        
    def decode(self, parser):
        while self._left:
            result = parser._get(self)
            if result is False:
                break
            self._left -= 1
            self._response.append(result)
        if not self._left:
            return self._response
        else:
            return False
        
    def resume(self, parser, result=None):
        if result is not None:
            self._left -= 1
            self._response.append(result)
        result = self.decode(parser)
        if result is not False and self.next:
            return self.next.resume(parser, result)
        else:
            return result
    
    
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
            return self._current.resume(self)
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
                return int(response)
            elif rtype == b'+':
                return response
            elif rtype == b'$':
                task = String(int(response), next)
                return task.decode(self)
            elif rtype == b'*':
                task = ArrayTask(int(response), next)
                return task.decode(self)
            else:
                # Clear the buffer and raise
                self._inbuffer = bytearray()
                raise self.protocolError()
        else:
            self._current = next
            return False
                
    def buffer(self):
        '''Current buffer'''
        return bytes(self._inbuffer)
    