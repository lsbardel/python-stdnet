import os
from datetime import datetime

from .models import User, AnonymousUser, Session

SESSION_USER_KEY = '_auth_user_id'
REDIRECT_FIELD_NAME = 'next'


def flush_session(request):
    s = request.session
    s.expiry = datetime.now()
    s.expired = True
    s.save()
    request.session = Session.objects.create()


def authenticate(username = None, password = None, **kwargs):
    try:
        user = User.objects.get(username=username)
        if user.check_password(password):
            return user
    except User.DoesNotExist:
        return None
    

def login(request, user):
    """Store the user id on the session
    """
    if user is None:
        user = request.user

    if SESSION_USER_KEY in request.session:
        if request.session[SESSION_USER_KEY] != user.id:
            flush_session(request)
    request.session[SESSION_USER_KEY] = user.id


def logout(request):
    flush_session(request)
    request.user = AnonymousUser()
    
    

def get_user(request):
    try:
        user_id = request.session[SESSION_USER_KEY]
    except KeyError:
        return AnonymousUser()
    try:
        return User.objects.get(id = user_id)
    except:
        flush_session(request)
        return AnonymousUser()


class SessionMiddleware(object):
    
    def process_request(self, request):
        cookie_name = os.environ.get('SESSION_COOKIE_NAME','stdnet-sessionid')
        session_key = request.COOKIES.get(cookie_name, None)
        if not (session_key and Session.objects.exists(session_key)):
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
