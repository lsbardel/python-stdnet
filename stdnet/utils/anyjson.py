import time
from datetime import date, datetime
try:
    import json
except:
    import simplejson as json
    
date2timestamp = lambda dte : int(time.mktime(dte.timetuple()))
    
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
