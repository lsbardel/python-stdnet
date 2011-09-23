'''\
Stdnet comes with a singleton setting instance which can be accessed by::

    from stdnet.conf import settings
    
The setting instance contains few default parameters used in throughout
the library. This parameters can be changed by the user by simply
overriding them.
    
.. attribute:: settings.DEFAULT_BACKEND

    the default :class:`stdnet.BackendDataServer` connection string. Check
    the :ref:`registering models <register-model>` documentation.
    
    Default ``"redis://127.0.0.1:6379/?db=7"``.
    
.. attribute:: DEFAULT_KEYPREFIX

    The prefix to prepend to all keys.
    
    Default ``"stdnet"``.
    
    
.. attribute:: settings.REDIS_PARSER

    The redis parser to choose. By default it is set to ``None`` which causes
    the library to choose the best possible available. More information
    is contained in the :ref:`redis parser <redis-parser>` documentation.
    
    Default ``None``.


.. attribute:: settings.MAX_CONNECTIONS

    The maximim number of connections to have opened at the same time.
    
    Default ``unlimited``.
    

To change settings::
    
    from stdnet.conf import settings
    
    settings.DEFAULT_BACKEND = 'redis://127.0.0.1:6379/?db=5'
'''
    
class Settings(object):

    def __init__(self):
        self.DEFAULT_BACKEND = 'redis://127.0.0.1:6379/?db=7'
        self.DEFAULT_KEYPREFIX  = 'stdnet.'
        self.CHARSET = 'utf-8'
        self.REDIS_PARSER = None
        self.MAX_CONNECTIONS = 2**31
        
        
settings = Settings()
