import base64
import os
import time
import random
import cPickle as pickle

from stdnet import orm
from stdnet import ObjectNotFound
from stdnet.utils.hash import md5_constructor

# Use the system (hardware-based) random number generator if it exists.
if hasattr(random, 'SystemRandom'):
    randrange = random.SystemRandom().randrange
else:
    randrange = random.randrange
MAX_SESSION_KEY = 18446744073709551616L     # 2 << 63


class SuspiciousOperation(Exception):
    pass


class EncodedPickledObjectField(orm.CharField):
    
    def to_python(self, value):
        encoded_data = base64.decodestring(self.data)
        pickled, tamper_check = encoded_data[:-32], encoded_data[-32:]
        if md5_constructor(pickled + os.environ.get('SESSION_SECRET_KEY')).hexdigest() != tamper_check:
            raise SuspiciousOperation("User tampered with session cookie.")
        try:
            return pickle.loads(pickled)
        except:
            return {}
        
    def serialise(self, value):
        pickled = pickle.dumps(session_dict)
        pickled_md5 = md5_constructor(pickled + os.environ.get('SESSION_SECRET_KEY')).hexdigest()
        return base64.encodestring(pickled + pickled_md5)


class SessionManager(orm.Manager):
    
    def create(self):
        return self.model(id = self.new_session_id()).save()
    
    def new_session_id(self):
        "Returns session key that isn't being used."
        # The random module is seeded when this Apache child is created.
        # Use settings.SECRET_KEY as added salt.
        try:
            pid = os.getpid()
        except AttributeError:
            pid = 1
        while 1:
            sk = os.environ.get('SESSION_SECRET_KEY')
            id = md5_constructor("%s%s%s%s" % (randrange(0, MAX_SESSION_KEY), pid, time.time(),sk)).hexdigest()
            if not self.exists(id):
                return id

    def exists(self, id):
        try:
            self.get(id = id)
        except ObjectNotFound:
            return False
        return True
    
    
class User(orm.StdModel):
    username = orm.SymbolField(unique = True)
    password = orm.CharField(required = True)
    
    def is_authenticated(self):
        return True
    
    
class AnonymousUser(object):
    
    def is_authenticated(self):
        return False


class Session(orm.StdModel):
    TEST_COOKIE_NAME = 'testcookie'
    TEST_COOKIE_VALUE = 'worked'
    
    id     = orm.SymbolField(primary_key=True)
    data   = orm.HashField()
    expiry = orm.DateTimeField(index = False, required = False)

    objects = SessionManager()
    
    def __str__(self):
        return self.id

    def set_test_cookie(self):
        self.data[self.TEST_COOKIE_NAME] = self.TEST_COOKIE_VALUE
        self.save()
