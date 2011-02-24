DEBUG = False
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3'
    }
}


INSTALLED_APPS  = ['django.contrib.auth',
                   'django.contrib.contenttypes',
                   'djangotestapp.testapp']

# Silence logging
import logging

class Silence(logging.Handler):
    def emit(self, record):
        pass

logging.getLogger("djangolink").addHandler(Silence())

