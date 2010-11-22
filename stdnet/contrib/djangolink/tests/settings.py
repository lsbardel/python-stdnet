DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'test-djstdnet.sqlite'
    }
}


INSTALLED_APPS  = ['django.contrib.auth',
                   'django.contrib.contenttypes',
                   'stdnet.contrib.djangolink',
                   'stdnet.contrib.djangolink.tests.testmodel']

# Silence logging
import logging

class Silence(logging.Handler):
    def emit(self, record):
        pass

logging.getLogger("djangolink").addHandler(Silence())

TEMPLATE_CONTEXT_PROCESSORS = (
            "django.contrib.auth.context_processors.auth",
            "django.contrib.messages.context_processors.messages"
            )
