import time
from datetime import date, datetime
from decimal import Decimal
import json
try:
    from numpy import ndarray
except ImportError:
    ndarray = None
    
from stdnet.lib.py2py3 import iteritems

JSPLITTER = '__'

date2timestamp = lambda dte : int(time.mktime(dte.timetuple()))


def totimestamp(dte):
    return time.mktime(dte.timetuple())


def totimestamp2(dte):
    return totimestamp(dte) + 0.000001*dte.microsecond


def todatetime(tstamp):
    return datetime.fromtimestamp(tstamp)    
    
    
class JSONDateDecimalEncoder(json.JSONEncoder):
    """The default JSON encoder used by stdnet. It provides
JSON serialization for three additional object, `datetime.date`,
`datetime.datetime` and `decimal.Decimal` from the standard library.
In addition if numpy_ is installed, it also provides serialization
of the ``ndarray`` into nested lists using the ``aslist`` function.

.. seealso:: It is the default encoder for :class:`stdnet.orm.JSONField`
"""
    def default(self, obj):
        if isinstance(obj,datetime):
            return {'__datetime__':totimestamp2(obj)}
        elif isinstance(obj, date):
            return {'__date__':totimestamp(obj)}
        elif isinstance(obj, Decimal):
            return {'__decimal__':str(obj)}
        elif ndarray and isinstance(obj,ndarray):
            return obj.tolist()
        else:
            return super(JSONDateDecimalEncoder,self).default(obj)


def date_decimal_hook(dct):
    '''The default JSON decoder hook. It is the inverse of
:class:`stdnet.utils.jsontools.JSONDateDecimalEncoder`.'''
    if '__datetime__' in dct:
        return todatetime(dct['__datetime__'])
    elif '__date__' in dct:
        return todatetime(dct['__date__']).date()
    elif '__decimal__' in dct:
        return Decimal(dct['__decimal__'])
    else:
        return dct
    
    
DefaultJSONEncoder = JSONDateDecimalEncoder
DefaultJSONHook = date_decimal_hook
    
class JSONRPCEncoder(json.JSONEncoder):
    """
    Provide custom serializers for JSON-RPC.
    """
    def default(self, obj):
        if isinstance(obj, date) or isinstance(obj, datetime):
            return date2timestamp(obj)
        else:
            raise exceptions.JSONEncodeException(\
                            "%r is not JSON serializable" % (obj,))
        

def nested_json_value(instance, attname, separator):
    '''Extract a values from a nested dictionary.

:parameter instance: and instance of an object.
:parameter attname:: the attribute name'''
    fields = attname.split(separator)
    data = getattr(instance,fields[0])
    for field in fields[1:]:
        data = data[field]
    if isinstance(data,dict):
        data = data['']
    return data


def flat_to_nested(data, instance = None, attname = None,
                   separator = None, loads = None):
    '''Convert a flat representation of a dictionary to
a nested representation. Fields in the flat representation are separated
by the *splitter* parameters.

:parameter data: a flat dictionary of key value pairs.
:parameter instance: optional instance of a model.
:parameter attribute: optional attribute of a model.
:parameter separator: optional separator. Default ``"__"``.
:parameter loads: optional data unserializer.
:rtype: a nested dictionary'''
    separator = separator or JSPLITTER
    val = {}
    flat_vals = {}
    for key in data:
        keys = key.split(separator)
        # first key equal to the attribute name
        if attname:
            if keys.pop(0) != attname:
                continue
        v = loads(data[key]) if loads else data[key]
        # if an instance is available, inject the flat attribute
        if not keys:
            if v is None:
                val = flat_vals = {}
                break
            else:
                continue
        else:
            flat_vals[key] = v
        
        d = val
        lk = keys[-1]
        for k in keys[:-1]:
            if k not in d:
                nd = {}
                d[k] = nd
            else:
                nd = d[k]
                if not isinstance(nd,dict):
                    nd = {'':nd}
                    d[k] = nd
            d = nd
        if lk not in d:
            d[lk] = v
        else:
            d[lk][''] = v
            
    if instance and flat_vals:
        for k,v in iteritems(flat_vals):
            setattr(instance,k,v)
            
    return val


def dict_flat_generator(value, attname = None, splitter = JSPLITTER,
                        dumps = None, prefix = None, error = ValueError,
                        recursive = True):
    '''Convert a nested dictionary into a flat dictionary representation'''
    if not isinstance(value,dict) or not recursive:
        if not prefix:
            raise error('Cannot assign a non dictionary to a JSON field')
        else:
            name = '{0}{1}{2}'.format(attname,splitter,prefix)\
                         if attname else prefix
            yield name,dumps(value) if dumps else value
    else:
        # loop over dictionary
        for field in value:
            val = value[field]
            key = prefix
            if field:
                key = '{0}{1}{2}'.format(prefix,splitter,field)\
                             if prefix else field
            for k,v2 in dict_flat_generator(val,attname,splitter,dumps,
                                            key,error, field):
                yield k,v2
