'''Simple chat application.

    python manage.py
    
and open web browsers at http://localhost:8060

To send messages from the JSON RPC open a python shell and::

    >>> from pulsar.apps import rpc
    >>> p = rpc.JsonProxy('http://127.0.0.1:8060/rpc')
    >>> p.message('Hi from rpc')
    'OK'
'''
import os
import sys
import json
from random import random
import time
try:
    import runtests
except ImportError: #pragma nocover
    sys.path.append('../../')
    import runtests
from pulsar import get_actor, Setting
from pulsar.apps import ws, wsgi, pubsub

from stdnet import getdb
from stdnet.conf import settings
from stdnet.lib.redis import PubSub

CHAT_DIR = os.path.dirname(__file__)

class StdnetServer(Setting):
    name = "server"
    flags = ["-s", "--server"]
    desc = 'Back-end data server where to run tests.'
    default = settings.DEFAULT_BACKEND
    
    
##    Web Socket Chat handler
class Chat(ws.WS):
    
    def on_open(self, request):
        # Add pulsar.connection environ extension to the set of active clients
        pubsub.add_client(request.cache['websocket'])
        
    def on_message(self, request, msg):
        if msg:
            lines = []
            for l in msg.split('\n'):
                l = l.strip()
                if l:
                    lines.append(l)
            msg = ' '.join(lines)
            if msg:
                pubsub.publish(msg)


class WebChat(wsgi.LazyWsgi):
    
    def __init__(self, name):
        self.name = name
        
    def setup(self):
        # Register a pubsub handler
        redis = getdb(get_actor().cfg.server, timeout=0).client
        pubsub.register_handler(PubSub(self.name, redis))
        return wsgi.WsgiHandler([wsgi.Router('/', get=self.home_page),
                                 ws.WebSocket('/message', Chat())])
        
    def home_page(self, request):
        data = open(os.path.join(CHAT_DIR, 'chat.html')).read()
        request.response.content_type = 'text/html'
        request.response.content = data % request.environ
        return request.response.start()


def server(callable=None, name=None, **kwargs):
    name = name or 'webchat'
    chat = WebChat(name)
    return wsgi.WSGIServer(name=name, callable=chat, **kwargs)


if __name__ == '__main__':  #pragma nocover
    server().start()
