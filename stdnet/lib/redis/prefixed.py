import redis



prefix_all = lambda pfix, args: ['%s%s' % (pfix, a) for a in args]
prefix_alternate = lambda pfix, args: [a if n//2*2==n else '%s%s' % (pfix, a)\
                                       for n, a in enumerate(args,1)]
prefix_not_last = lambda pfix, args: ['%s%s' % (pfix, a) for a in args[:-1]]\
                                        + [args[-1]]
prefix_not_first = lambda pfix, args: [args[0]] +\
                                      ['%s%s' % (pfix, a) for a in args[1:]]

def prefix_zinter(pfix, args):
    dest, numkeys, params = args[0], args[1], args[2:]
    args = ['%s%s' % (pfix, dest), numkeys]
    nk = 0
    for p in params:
        if nk < numkeys:
            nk += 1
            p = '%s%s' % (pfix, p)
        args.append(p)
    return args

def prefix_sort(pfix, args):
    prefix = True
    nargs = []
    for a in args:
        if prefix:
            a = '%s%s' % (pfix, a)
            prefix = False
        elif a in ('BY', 'GET', 'STORE'):
            prefix = True
        nargs.append(a)
    return nargs
    
def pop_list_result(pfix, result):
    if result:
        return (result[0][len(pfix):], result[1])
    
    
class BasePrefixed(object):
    '''A class for a prefixed redis client. It append a prefix to all keys.

.. attribute:: prefix

    The prefix to append to all keys
    
'''    
    EXCLUDE_COMMANDS = frozenset(('BGREWRITEOF', 'BGSAVE', 'CLIENT', 'CONFIG',
                                  'DBSIZE', 'DEBUG', 'DISCARD', 'ECHO',
                                  'EVAL', 'EVALSHA', 'EXEC',
                                  'INFO', 'LASTSAVE', 'PING',
                                  'PSUBSCRIBE', 'PUBLISH', 'PUNSUBSCRIBE',
                                  'QUIT', 'RANDOMKEY', 'SAVE', 'SCRIPT',
                                  'SELECT', 'SHUTDOWN', 'SLAVEOF', 
                                  'SLOWLOG', 'SUBSCRIBE', 'SYNC',
                                  'TIME', 'UNSUBSCRIBE', 'UNWATCH'))
    SPECIAL_COMMANDS = {
        'BITOP': prefix_not_first,
        'BLPOP': prefix_not_last,
        'BRPOP': prefix_not_last,
        'BRPOPLPUSH': prefix_not_last,
        'RPOPLPUSH': prefix_all,
        'DEL': prefix_all,
        'FLUSHDB': lambda prefix, args: raise_error(),
        'FLUSHALL': lambda prefix, args: raise_error(),
        'MGET': prefix_all,
        'MSET': prefix_alternate,
        'MSETNX': prefix_alternate,
        'MIGRATE': prefix_all,
        'RENAME': prefix_all,
        'RENAMENX': prefix_all,
        'SDIFF': prefix_all,
        'SDIFFSTORE': prefix_all,
        'SINTER': prefix_all,
        'SINTERSTORE': prefix_all,
        'SMOVE': prefix_not_last,
        'SORT': prefix_sort,
        'SUNION': prefix_all,
        'SUNIONSTORE': prefix_all,
        'WATCH': prefix_all,
        'ZINTERSTORE': prefix_zinter,
        'ZUNIONSTORE': prefix_zinter
    }
    RESPONSE_CALLBACKS = {
        'KEYS': lambda pfix, response: [r[len(pfix):] for r in response],
        'BLPOP': pop_list_result,
        'BRPOP': pop_list_result
    }
    def __init__(self, client, prefix):
        super(BasePrefixed, self).__init__(client)
        self.__prefix = prefix
        
    @property
    def prefix(self):
        return self.__prefix
    
    def preprocess_command(self, cmnd, *args, **options):
        if cmnd not in self.EXCLUDE_COMMANDS:
            handle = self.SPECIAL_COMMANDS.get(cmnd, self.handle)
            args = handle(self.prefix, args)
        return args, options
    
    def handle(self, prefix, args):
        if args:
            args = list(args)
            args[0] = '%s%s' % (prefix, args[0])
        return args
        
    def dbsize(self):
        return self.client.countpattern('%s*' % self.prefix)
    
    def flushdb(self):
        return self.client.delpattern('%s*' % self.prefix)
    
    def _eval(self, command, body, keys, *args, **options):
        if keys:
            keys = ['%s%s' % (self.prefix, k) for k in keys]
        return super(PrefixedRedis, self)._eval(command, body, keys, *args,
                                                **options)
        
    def _parse_response(self, request, response, command_name, args, options):
        if command_name in self.RESPONSE_CALLBACKS:
            if not isinstance(response, Exception):
                response = self.RESPONSE_CALLBACKS[command_name](self.prefix,
                                                                 response)
        return self.client._parse_response(request, response, command_name,
                                           args, options)
    
