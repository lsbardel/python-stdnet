
from djpcms.views import appsite

from stdnet.contrib.djstdnet.views import RedisHomeView, RedisDbView, RedisDbFlushView


class StdnetMonitorApplication(appsite.ApplicationBase):
    name = 'Stdnet Monitor'
    list_per_page = 100
    
    home  = RedisHomeView(isplugin = True, isapp = True)
    db    = RedisDbView(regex = '(?P<db>\d+)', isapp = True)
    flush = RedisDbFlushView(regex = 'flush', parent = 'db')
    
    def dburl(self, db):
        dbview = self.getview('db')
        djp = view(request, db = db)
        return djp.url