from redis.connection import PythonParser as _p, InvalidResponse

from stdnet.utils.conf import settings

from .extensions import *
from .client import *
from .info import *
from . import parser

EXCEPTION_CLASSES = _p.EXCEPTION_CLASSES
HAS_C_EXTENSIONS = False

try:
    from . import cparser
    HAS_C_EXTENSIONS = True
except ImportError:
    cparser = parser

try:
    from .async import AsyncConnectionPool
except ImportError:
    AsyncConnectionPool = None
    
def ResponseError(response):
    "Parse an error response"
    response = response.split(' ')
    error_code = response[0]
    if error_code not in EXCEPTION_CLASSES:
        error_code = 'ERR'
    response = ' '.join(response[1:])
    return EXCEPTION_CLASSES[error_code](response)


PythonRedisParser = lambda : parser.RedisParser(InvalidResponse, ResponseError)
CppRedisParser = lambda : cparser.RedisParser(InvalidResponse, ResponseError)

def RedisParser(type=None):
    if settings.REDIS_PY_PARSER or type=='py':
        return PythonRedisParser()
    else:
        return CppRedisParser()


def redis_client(address, connection_pool=None, timeout=None, reader=None,
                 **kwargs):
    '''Get a new redis client'''
    if not connection_pool:
        if timeout == 0:
            if not AsyncConnectionPool:
                raise ImportError('Asynchronous connection requires async '
                                  'bindings installed.')
            connection_pool = AsyncConnectionPool
        else:
            connection_pool = ConnectionPool
        kwargs['parser'] = lambda: RedisParser(reader)
        connection_pool = connection_pool(address, **kwargs)
    return Redis(connection_pool=connection_pool)