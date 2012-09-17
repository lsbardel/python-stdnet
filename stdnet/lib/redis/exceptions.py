"Core exceptions raised by the Redis client"
import stdnet


class RedisError(Exception):
    pass

class RedisConnectionError(stdnet.ConnectionError):
    def __init__(self, msg='', retry=False):
        self.retry = retry
        super(RedisConnectionError,self).__init__(msg)
        

class RedisConnectionTimeout(RedisConnectionError):
    pass


class RedisProtocolError(stdnet.ResponseError):
    pass


class RedisInvalidResponse(stdnet.ResponseError):
    pass


class AuthenticationError(RedisInvalidResponse):
    pass


class NoScriptError(RedisInvalidResponse):
    msg = 'NOSCRIPT No matching script. Please use EVAL.'
    def __repr__(self):
        return self.msg
    __str__ = __repr__


class ScriptError(RedisInvalidResponse):

    def __init__(self, command, name, msg):
        msg = 'Error while executing {0} command on "{1}" script. {2}'\
        .format(command,name,msg)
        super(ScriptError,self).__init__(msg)


