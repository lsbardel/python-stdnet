from datetime import datetime

from django.contrib.sessions.backends.base import SessionBase
from django.conf import settings

from stdnet import ObjectNotFound
from stdnet.contrib.sessions.models import Session, session_settings

session_settings['SECRET_KEY'] = settings.SECRET_KEY

now = datetime.now


class SessionStore(SessionBase):
    """
    Implements Redis session store.
    """

    def exists(self, session_key):
        if db.exists("session:%s" % session_key):
            return True
        return False

    def create(self):
        s =  Session.objects.create()
        self.session_key = s.id
        return s

    def save(self, must_create=False):
        if must_create:
            func = db.setnx
        else:
            func = db.set
        key = "session:%s" % self.session_key
        result = func(
                key,
                self.encode(self._get_session(no_load=must_create)))
        if must_create and result is None:
            raise CreateError
        # The key has been set, set its expiration
        db.expire(key, self.get_expiry_age())

    def delete(self, session_key=None):
        if session_key is None:
            session_key = self.session_key
        db.delete("session:%s" % session_key)

    def load(self):
        session_data = db.get("session:%s" % self.session_key)
        if session_data is None:
            self.create()
            return {}
        return self.decode(session_data)
    