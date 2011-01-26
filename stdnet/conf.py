
class Settings(object):
    '''The setting class contains configuration parameters used in stdnet.
    
    .. attribute:: DEFAULT_BACKEND
    
        the default :class:`stdnet.BackendDataServer` connection string, Default ``"redis://127.0.0.1:6379/?db=7"``.
        
    .. attribute:: DEFAULT_KEYPREFIX
    
        The prefix to prepend to all keys. Default ``"stdnet"``.
        
    .. attribute:: SCHEMA
    
        This defines how instances of models are stored on the back-end server.
        It is only used when a Redis back-end server is used. Possible choices::
        
            * hash
            * compact-hash (default)
                
To change settings::
    
    from stdnet.conf import settings
    
    settings.DEFAULT_BACKEND = 'redis://127.0.0.1:6379/?db=5'
    '''
    def __init__(self):
        self.DEFAULT_BACKEND    = 'redis://127.0.0.1:6379/?db=7'
        self.DEFAULT_KEYPREFIX  = 'stdnet.'
        self.SCHEMA = 'compact-hash'
        
        
settings = Settings()
