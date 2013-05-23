def raise_error(exception=NotImplementedError):
    raise exception()

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

def prefix_eval_keys(pfix, args):
    n = args[1]
    if n:
        keys = tuple(('%s%s' % (pfix, a) for a in args[2:n+2]))
        return args[:2] + keys + args[n+2:]
    else:
        return args
        