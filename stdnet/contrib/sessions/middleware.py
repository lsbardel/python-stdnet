import os
from .models import AnonymousUser, Session

SESSION_KEY = '_auth_user_id'
BACKEND_SESSION_KEY = '_auth_user_backend'
REDIRECT_FIELD_NAME = 'next'


def get_user(request):
    try:
        user_id = request.session.data.get(SESSION_KEY)
        try:
            user = Session.objects.get(user_id)
        except:
            raise KeyError
    except KeyError:
        user = AnonymousUser()
    return user


class SessionMiddleware(object):
    
    def process_request(self, request):
        site = request.site
        cookie_name = os.environ.get('SESSION_COOKIE_NAME','stdnet-sessionid')
        session_key = request.COOKIES.get(cookie_name, None)
        if not session_key or Session.objects.exists(session_key):
            session = Session.objects.create()
            session.modified = True
        else:
            session = Session.objects.get(id = session_key)
            session.modified = False
        request.session = session
        request.user = get_user(request)
        return None
    
    
    def process_response(self, request, response):
        """
        If request.session was modified, or if the configuration is to save the
        session every time, save the changes and set a session cookie.
        """
        session = request.session
        modified = getattr(session,'modified',True)
        if modified:
            cookie_name = os.environ.get('SESSION_COOKIE_NAME','stdnet-sessionid')
            response.set_cookie(cookie_name, session.id)
        return response
