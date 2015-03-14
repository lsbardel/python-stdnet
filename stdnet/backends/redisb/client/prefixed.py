def raise_error(exception=NotImplementedError):
    raise exception()

prefix_all = lambda pfix, args: ['%s%s' % (pfix, a) for a in args]
prefix_alternate = lambda pfix, args: [a if n // 2 * 2 == n else '%s%s' % (pfix, a)
                                       for n, a in enumerate(args, 1)]
prefix_not_last = lambda pfix, args: ['%s%s' % (pfix, a)
                                      for a in args[:-1]] + [args[-1]]
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


def prefix_eval_keys(pfix, args):
    n = args[1]
    if n:
        keys = tuple(('%s%s' % (pfix, a) for a in args[2:n + 2]))
        return args[:2] + keys + args[n + 2:]
    else:
        return args


class PrefixedRedisMixin(object):

    '''A class for a prefixed redis client. It append a prefix to all keys.

.. attribute:: prefix

    The prefix to append to all keys

'''
    EXCLUDE_COMMANDS = frozenset(('BGREWRITEOF', 'BGSAVE', 'CLIENT', 'CONFIG',
                                  'DBSIZE', 'DEBUG', 'DISCARD', 'ECHO', 'EXEC',
                                  'INFO', 'LASTSAVE', 'PING',
                                  'PSUBSCRIBE', 'PUBLISH', 'PUNSUBSCRIBE',
                                  'QUIT', 'RANDOMKEY', 'SAVE', 'SCRIPT LOAD',
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
        'EVAL': prefix_eval_keys,
        'EVALSHA': prefix_eval_keys,
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
        self._client = client
        self._prefix = prefix

    @property
    def client(self):
        return self._client

    @property
    def prefix(self):
        return self._prefix

    @property
    def connection_pool(self):
        return self._client.connection_pool

    def address(self):
        return self._client.address()

    def execute_command(self, cmnd, *args, **options):
        "Execute a command and return a parsed response"
        args, options = self.preprocess_command(cmnd, *args, **options)
        return self.client.execute_command(cmnd, *args, **options)

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

    def _parse_response(self, request, response, command_name, args, options):
        if command_name in self.RESPONSE_CALLBACKS:
            if not isinstance(response, Exception):
                response = self.RESPONSE_CALLBACKS[command_name](self.prefix,
                                                                 response)
        return self.client._parse_response(request, response, command_name,
                                           args, options)
