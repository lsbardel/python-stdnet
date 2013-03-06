import redis
from redis.client import pairs_to_dict

__all__ = ['pairs_to_dict', 'Redis', 'ConnectionPool']

ConnectionError = redis.ConnectionError

#redis_before_send = Signal()
#redis_after_receive = Signal()


class ConnectionPool(redis.ConnectionPool):
    '''Synchronous Redis connection pool'''
    def __init__(self, address, **kwargs):
        if isinstance(address, tuple):
            host, port = address
            kwargs['host'] = host
            kwargs['port'] = port
        else:
            kwargs['path'] = address
        super(ConnectionPool, self).__init__(**kwargs)
        
    def request(self, client, *args, **options):
        command_name = args[0]
        connection = self.get_connection(command_name, **options)
        try:
            connection.send_command(*args)
            return client.parse_response(connection, command_name, **options)
        except ConnectionError:
            connection.disconnect()
            connection.send_command(*args)
            return client.parse_response(connection, command_name, **options)
        finally:
            self.release(connection)
            
    
class Redis(redis.StrictRedis):
    
    def execute_command(self, *args, **options):
        "Execute a command and return a parsed response"
        return self.connection_pool.request(self, *args, **options)
    
    def pipeline(self, transaction=True, shard_hint=None):
        return StrictPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint)