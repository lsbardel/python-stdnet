import time
from datetime import date, datetime
from decimal import Decimal
import json
    
date2timestamp = lambda dte : int(time.mktime(dte.timetuple()))


def totimestamp(dte):
    return time.mktime(dte.timetuple())

def totimestamp2(dte):
    return totimestamp(dte) + 0.000001*dte.microsecond

def todatetime(tstamp):
    return datetime.fromtimestamp(tstamp)
    
    
def json_compact(data, sep = '_'):
    if not sep:
        return data
    d = {}
    for k,v in data.items():
        rdata = d
        keys = k.split(sep)
        for key in keys[:-1]:
            kd = rdata.get(key,None)
            if kd is None:
                kd = {}
                rdata[key] = kd
            rdata = kd
        rdata[keys[-1]] = v
    return d
    
    
class JSONDateDecimalEncoder(json.JSONEncoder):
    """
    Provide custom serializers for JSON-RPC.
    """
    def default(self, obj):
        if isinstance(obj,datetime):
            return {'__datetime__':totimestamp2(obj)}
        elif isinstance(obj, date):
            return {'__date__':totimestamp(obj)}
        elif isinstance(obj, Decimal):
            return {'__decimal__':str(obj)}
        else:
            raise ValueError("%r is not JSON serializable" % (obj,))


def date_decimal_hook(dct):
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
            raise exceptions.JSONEncodeException("%r is not JSON serializable" % (obj,))
        

class jsonPickler(object):
    
    def dumps(self, obj, **kwargs):
        return json.dumps(res, cls=JSONRPCEncoder, **kwargs)
    
    def loads(self,sobj):
        return json.loads(sobj)
