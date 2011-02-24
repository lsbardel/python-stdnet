import base64
import os
import time
import random
import hashlib

from stdnet import orm
from stdnet import ObjectNotFound
from stdnet.utils import pickle, to_bytestring

# Use the system (hardware-based) random number generator if it exists.
if hasattr(random, 'SystemRandom'):
    randrange = random.SystemRandom().randrange
else:
    randrange = random.randrange
MAX_SESSION_KEY = 18446744073709551616     # 2 << 63

UNUSABLE_PASSWORD = '!' # This will never be a valid hash


def get_hexdigest(salt, raw_password):
    return hashlib.sha1(salt + raw_password).hexdigest()


def check_password(raw_password, enc_password):
    """
    Returns a boolean of whether the raw_password was correct. Handles
    encryption formats behind the scenes.
    """
    salt, hsh = enc_password.split('$')
    return hsh == get_hexdigest(salt, raw_password)


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
        pickled_sha = md5_constructor(pickled + os.environ.get('SESSION_SECRET_KEY')).hexdigest()
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
            sk = os.environ.get('SESSION_SECRET_KEY') or ''
            val = to_bytestring("%s%s%s%s" % (randrange(0, MAX_SESSION_KEY), pid, time.time(),sk))
            id = hashlib.sha1(val).hexdigest()
            if not self.exists(id):
                return id

    def exists(self, id):
        try:
            self.get(id = id)
        except ObjectNotFound:
            return False
        return True
    
    
class UserManager(orm.Manager):
    
    def create_user(self, username, password=None, email=None, is_superuser = False):
        if email:
            try:
                email_name, domain_part = email.strip().split('@', 1)
            except ValueError:
                pass
            else:
                email = '@'.join([email_name, domain_part.lower()])
        else:
            email = ''

        user = self.model(username=username,
                          #email=email,
                          is_superuser=is_superuser)

        user.set_password(password)
        return user.save()

    def create_superuser(self, username, password = None, email = None):
        return self.create_user(username, email, password, is_superuser = True)
    
    
class User(orm.StdModel):
    username = orm.SymbolField(unique = True)
    password = orm.CharField(required = True)
    is_active = orm.BooleanField(default = True)
    is_superuser = orm.BooleanField(default = False)
    
    objects = UserManager()
    
    def __unicode__(self):
        return self.username
    
    def is_authenticated(self):
        return True
    
    def set_password(self, raw_password):
        if raw_password:
            salt = get_hexdigest(str(random.random()), str(random.random()))[:5]
            hsh = get_hexdigest(salt, raw_password)
            self.password = '%s$%s' % (salt, hsh)
        else:
            self.set_unusable_password()
            
    def set_unusable_password(self):
        # Sets a value that will never be a valid hash
        self.password = UNUSABLE_PASSWORD
            
    def check_password(self, raw_password):
        """Returns a boolean of whether the raw_password was correct."""
        return check_password(raw_password, self.password)
    
    @classmethod
    def login(cls, request, user):
        pass
    
    @classmethod
    def logout(cls, request):
        pass
    
    
class AnonymousUser(object):
    '''Anonymous user ala django'''
    is_active = False
    is_superuser = False
    
    def is_authenticated(self):
        return False
    

class Group(orm.StdModel):
    '''simple group'''
    name  = orm.SymbolField(unique = True)
    users = orm.ManyToManyField(User, related_name = 'groups')
    

class Permission(orm.StdModel):
    '''A general permission model'''
    numeric_code = orm.IntegerField()
    group = orm.ForeignKey(Group, required = False)
    user = orm.ForeignKey(User, required = False)
    model_or_object = orm.SymbolField()
    

class Session(orm.StdModel):
    TEST_COOKIE_NAME = 'testcookie'
    TEST_COOKIE_VALUE = 'worked'
    
    '''A simple session model with instances living in Redis.'''
    id = orm.SymbolField(primary_key=True)
    data = orm.HashField()
    started = orm.DateTimeField(index = False, required = False)
    expiry = orm.DateTimeField(index = False, required = False)
    expired = orm.BooleanField(default = False)
    modified = True
    
    objects = SessionManager()
    
    def __str__(self):
        return self.id
    
    def __contains__(self, key):
        return key in self.data
    
    def __getitem__(self, key):
        return self.data[key]
    
    def __setitem__(self, key, val):
        self.data[key] = val
        self.data.save()
        
    def __delitem__(self, key):
        del self.data[key]
        self.data.save()

    def set_test_cookie(self):
        self[self.TEST_COOKIE_NAME] = self.TEST_COOKIE_VALUE
        self.data.save()

    def test_cookie_worked(self):
        return self.get(self.TEST_COOKIE_NAME) == self.TEST_COOKIE_VALUE

    def delete_test_cookie(self):
        del self[self.TEST_COOKIE_NAME]
        self.data.save()
    
