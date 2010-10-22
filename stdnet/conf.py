
class Settings(object):
    '''The setting class contains configuration parameters used in stdnet.
    
    .. attribute:: DEFAULT_BACKEND
    
        the default :class:`stdnet.BackendDataServer` connection string, Default ``"redis://127.0.0.1:6379/?db=7"``.
        
    .. attribute:: DEFAULT_KEYPREFIX
    
        The prefix to prepend to all keys. Default ``"stdnet"``.
                
To change settings::
    
    from stdnet.conf import settings
    
    settings.DEFAULT_BACKEND = 'redis://127.0.0.1:6379/?db=5'
    '''
    def __init__(self):
        self.DEFAULT_BACKEND    = 'redis://127.0.0.1:6379/?db=7'
        self.DEFAULT_KEYPREFIX  = 'stdnet'
        
        
settings = Settings()
