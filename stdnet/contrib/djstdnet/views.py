from datetime import datetime, timedelta
from urllib import urlencode

from djpcms.views import appview
from djpcms.utils.ajax import jhtmls
from djpcms.utils import mark_safe
from djpcms import forms
from djpcms.template import loader
from django.utils.dateformat import format, time_format

from stdnet import orm
from stdnet.utils.format import format_number

import redis


class RedisForm(forms.Form):
    server = forms.CharField(initial = 'localhost')
    port = forms.IntegerField(initial = 6379)
    db = forms.IntegerField(initial = 0, required = False)


def niceadd(l,name,value):
    if value is not None:
        l.append({'name':name,'value':value})


def nicedate(t):
    try:
        from django.conf import settings
        d = datetime.fromtimestamp(t)
        return '%s %s' % (format(d.date(),settings.DATE_FORMAT),time_format(d.time(),settings.TIME_FORMAT)) 
    except:
        return ''

    
fudge  = 1.25
hour   = 60.0 * 60.0
day    = hour * 24.0
week   = 7.0 * day
month  = 30.0 * day
def nicetimedelta(t):
    tdelta = timedelta(seconds = t)
    days    = tdelta.days
    sdays   = day * days
    delta   = tdelta.seconds + sdays
    if delta < fudge:
        return u'about a second'
    elif delta < (60.0 / fudge):
        return u'about %d seconds' % int(delta)
    elif delta < (60.0 * fudge):
        return u'about a minute'
    elif delta < (hour / fudge):
        return u'about %d minutes' % int(delta / 60.0)
    elif delta < (hour * fudge):
        return u'about an hour'
    elif delta < day:
        return u'about %d hours' % int(delta / hour)
    elif days == 1:
        return u'about 1 day'
    else:
        return u'about %s days' % days


def getint(v):
    try:
        return int(v)
    except:
        return None

    

class RedisHomeView(appview.AppViewBase):
    plugin_form = RedisForm
    
    def render(self,
               djp,
               server = 'localhost',
               port = 6379,
               db = 0,
               **kwargs):
        r = redis.Redis(host = server, port = port, db = db)
        urldata = urlencode({'server':server,'port':port})
        info = r.info()
        info1 = []
        info2 = []
        databases = {}
        databases['header'] = ('db','Keys','Expires','Commands')
        databases['body']   = info2
        model_info = []
        keys = 0
        dbs = {}
        dd  = {'baseurl':djp.url,
               'server': urldata,
               'cl': 'class="%(ajax)s %(nicebutton)s"' % djp.css._dict}
        # Databases
        for k in info:
            if k[:2] == 'db':
                num = getint(k[2:])
                if num is not None:
                    dbs[num] = info[k]
        for n in sorted(dbs.keys()):
            dd['db'] = n
            data = dbs[n]
            keydb = data['keys']
            link  = mark_safe('<a href="%(baseurl)s%(db)s/?%(server)s" title="database %(db)s">%(db)s</a>' % dd)
            flush = mark_safe('<a %(cl)s href="%(baseurl)s%(db)s/flush/?%(server)s" title="flush database %(db)s">flush</a>' % dd)
            info2.append((link,{'class':'redisdb%s keys' % n, 'value':format_number(keydb)},data['expires'],flush))
            keys += keydb
        niceadd(info1, 'Redis version', info['redis_version'])
        niceadd(info1, 'Process id', info['process_id'])
        niceadd(info1, 'Total keys', format_number(keys))
        niceadd(info1, 'Memory used', info['used_memory_human'])
        niceadd(info1, 'Up time', nicetimedelta(info['uptime_in_seconds']))
        niceadd(info1, 'Virtual Memory enabled', 'yes' if info['vm_enabled'] else 'no')
        niceadd(info1, 'Last save', nicedate(info['last_save_time']))
        niceadd(info1, 'Commands processed', format_number(info['total_commands_processed']))
        niceadd(info1, 'Connections received', format_number(info['total_connections_received']))
        
        model_header = ['name','db','base key']
        for model in orm.mapper._registry:
            meta = model._meta
            cursor = meta.cursor
            if cursor.name == 'redis':
                model_info.append([meta, cursor.db, meta.basekey()])
        return loader.render_to_string('djstdnet/redis_monitor.html',
                                       {'info1':info1,
                                        'databases':databases,
                                        'model_header':model_header,
                                        'model_info':model_info})
        

class RedisDbView(appview.AppViewBase):
    
    def get_db(self, djp):
        db = djp.kwargs.get('db',0)
        data = dict(djp.request.GET.items())
        return redis.Redis(host = data.get('server','localhost'),
                           port = int(data.get('port',6379)),
                           db = int(db)),data

    def render(self, djp, **kwargs):
        r,data = self.get_db(djp)
        p = data.get('page',1)
        keys = {'header': ('name','type','tyme to expiry','delete'),
                'body': self.keys(r,p)}
        return loader.render_to_string('djstdnet/redis_db.html',
                                       {'keys':keys})
        
    def keys(self, r, p):
        kpp = self.appmodel.keys_par_page
        keys = r.keys()
        for key in keys:
            yield key,r.type(key),r.ttl(key),''


class RedisDbFlushView(RedisDbView):
    
    def default_post(self, djp):
        r,data = self.get_db(djp)
        r.flushdb()
        keys = len(r.keys())
        return jhtmls(identifier = 'td.redisdb%s.keys' % r.db, html = format_number(keys))

