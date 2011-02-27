from urllib import urlencode
from datetime import datetime, timedelta

from djpcms import forms
from djpcms.views import appview
from djpcms.utils.html import Paginator
from djpcms.utils.ajax import jhtmls, jredirect
from djpcms.utils import lazyattr, gen_unique_id
from djpcms.template import loader, mark_safe
from django.utils.dateformat import format, time_format

from stdnet import orm
from stdnet.utils.format import format_number
from stdnet.lib import redis


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

    

class RedisHomeView(appview.View):
    plugin_form = RedisForm
    
    def render(self, djp):
        kwargs = djp.kwargs
        server = kwargs.get('server','localhost')
        port   = kwargs.get('port',6379)
        db     = kwargs.get('db', 0)
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
        return loader.render_to_string('monitor/redis_monitor.html',
                                       {'info1':info1,
                                        'databases':databases,
                                        'model_header':model_header,
                                        'model_info':model_info})
        

def type_length(r, key):
    typ = r.type(key)
    l = 1
    if typ == 'set':
        l = r.scard(key)
    elif typ == 'list':
        l = r.llen(key)
    elif typ == 'hash':
        l = r.hlen(key)
    return typ,l
        
class DbQuery(object):
    
    def __init__(self, djp, r):
        self.djp   = djp
        self.r     = r
        
    def count(self):
        return self.r.dbsize()
    
    @lazyattr
    def data(self):
        return self.r.keys()
    
    def __getitem__(self, slic):
        data = self.data()[slic]
        r = self.r
        for key in data:
            typ,len = type_length(r, key)
            yield key,typ,len,r.ttl(key),''
        
        
class RedisDbView(appview.View):
    '''Display information about keys in one database.'''
    def get_db(self, djp):
        db = djp.kwargs.get('db',0)
        data = dict(djp.request.GET.items())
        return redis.Redis(host = data.get('server','localhost'),
                           port = int(data.get('port',6379)),
                           db = int(db))

    def title(self, djp):
        return 'Database {0[db]}'.format(djp.kwargs)
    
    def render(self, djp):
        request = djp.request
        appmodel = self.appmodel
        r = self.get_db(djp)
        query = DbQuery(djp,r)
        p = Paginator(request, query, per_page = appmodel.list_per_page)
        c = {'paginator': p,
             'id':gen_unique_id(),
             'db':r.db,
             'djp':djp,
             'appmodel': appmodel,
             'headers': ('name','type','length','time to expiry','delete'),
             'items':p.qs}
        return loader.render_to_string(['monitor/pagination.html',
                                        'djpcms/components/pagination.html'],c)
        


class RedisDbFlushView(RedisDbView):
    
    def default_post(self, djp):
        r = self.get_db(djp)
        r.flushdb()
        keys = len(r.keys())
        return jhtmls(identifier = 'td.redisdb%s.keys' % r.db, html = format_number(keys))


class StdModelInformationView(appview.ModelView):
    
    def __init__(self, **kwargs):
        kwargs['isplugin'] = True
        super(StdModelInformationView,self).__init__(**kwargs)
        
    '''Display Information regarding a
    :class:`stdnet.orm.StdModel` registered with a backend database.'''
    def render(self, djp, **kwargs):
        meta = self.model._meta
        return loader.render_to_string('monitor/stdmodel.html',{'meta':meta})


class StdModelDeleteAllView(appview.ModelView):
    _methods = ('post',)
    
    def default_post(self, djp):
        self.model.flush()
        next,curr = forms.next_and_current(djp.request)
        return jredirect(next or curr)
