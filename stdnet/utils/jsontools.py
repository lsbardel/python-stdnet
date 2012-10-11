import time
from datetime import date, datetime
from decimal import Decimal
import json
    
from stdnet.utils import iteritems


__ALL__ = ['JSPLITTER','EMPTYJSON',
           'date2timestamp','totimestamp',
           'totimestamp2','todatetime',
           'JSONDateDecimalEncoder','date_decimal_hook',
           'DefaultJSONEncoder','DefaultJSONHook',
           'flat_to_nested','dict_flat_generator',
           'addmul_number_dicts']

JSPLITTER = '__'
EMPTYJSON = (b'', '', None)
date2timestamp = lambda dte : int(time.mktime(dte.timetuple()))


def totimestamp(dte):
    return time.mktime(dte.timetuple())


def totimestamp2(dte):
    return totimestamp(dte) + 0.000001*dte.microsecond


def todatetime(tstamp):
    return datetime.fromtimestamp(tstamp)    
    
    
class JSONDateDecimalEncoder(json.JSONEncoder):
    """The default JSON encoder used by stdnet. It provides
JSON serialization for four additional classes:

* `datetime.date` as a ``{'__date__': timestamp}`` dictionary
* `datetime.datetime` as a ``{'__datetime__': timestamp}`` dictionary
* `decimal.Decimal` as a ``{'__decimal__': number}`` dictionary

.. seealso:: It is the default encoder for :class:`stdnet.odm.JSONField`
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
    for key,value in iteritems(data):
        if value is None:
            continue
        keys = key.split(separator)
        # first key equal to the attribute name
        if attname:
            if keys.pop(0) != attname:
                continue
        if loads:
            value = loads(value)
        # if an instance is available, inject the flat attribute
        if not keys:
            if value is None:
                val = flat_vals = {}
                break
            else:
                continue
        else:
            flat_vals[key] = value
        
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
            d[lk] = value
        else:
            d[lk][''] = value
            
    if instance and flat_vals:
        for attr, value in iteritems(flat_vals):
            setattr(instance, attr, value)
            
    return val


def dict_flat_generator(value, attname=None, splitter=JSPLITTER,
                        dumps=None, prefix=None, error=ValueError,
                        recursive=True):
    '''Convert a nested dictionary into a flat dictionary representation'''
    if not isinstance(value, dict) or not recursive:
        if not prefix:
            raise error('Cannot assign a non dictionary to a JSON field')
        else:
            name = '%s%s%s' % (attname,splitter,prefix) if attname else prefix
            yield name, dumps(value) if dumps else value
    else:
        # loop over dictionary
        for field in value:
            val = value[field]
            key = prefix
            if field:
                key = '%s%s%s' % (prefix,splitter,field) if prefix else field
            for k, v2 in dict_flat_generator(val,attname,splitter,dumps,
                                             key,error, field):
                yield k,v2


def value_type(data):
    v = None
    for d in data:
        typ = 0
        if isinstance(d,(tuple,list)):
            typ = 1
        elif isinstance(d,dict):
            typ = 2
        if v is None:
            v = typ
        elif v != typ:
            raise ValueError('Inconsistent types')
    return v

  
def addmul_number_dicts(*series):
    '''Utility function for multiplying dictionary by a numeric value and
add the results.

:parameter series: a tuple of two elements tuples.
    Each serie is of the form::
    
        (weight,dictionary)
        
    where ``weight`` is a number and ``dictionary`` is a dictionary with
    numeric values.
    
Only common fields are aggregated.
'''
    if not series:
        return
    vtype = value_type((s[1] for s in series))
    if vtype == 0:
        return sum((weight*d for weight,d in series))
    elif vtype == 2:
        keys = set(series[0][1])
        for serie in series[1:]:
            keys.intersection_update(serie[1])
        result = {}
        for key in keys:
            key_series = tuple((weight,d[key]) for weight,d in series)
            result[key] = addmul_number_dicts(*key_series)
        return result
    