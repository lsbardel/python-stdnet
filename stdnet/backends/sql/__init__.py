'''An utility for managing sql models with the same :class:`stdnet.odm.Router`
as key-valued pairs models.'''
try:
    import sqlalchemy as sql
except ImportError:
    raise ImportError('Sql backend requires SqlAlchemy')

from sqlalchemy.orm import sessionmaker

import stdnet
from stdnet import odm
from stdnet.backends import get_connection_string


class Manager(odm.Manager):
     
    def session_factory(self, backend, router=None):
        session = backend.Session()
        session._router = router
        return session
    
    def new(self, *args, **kwargs):
        instance = self.model(*args, **kwargs)
        session = self.session()
        session.add(instance)
        session.flush()
        return instance
        
    def create_all(self):
        self.model.metadata.create_all(self.backend.client)
    
    
class BackendDataServer(stdnet.BackendDataServer):
    default_manager = Manager
        
    def __init__(self, name=None, address=None, **opts):
        super(BackendDataServer, self).__init__(name, address, **opts)
        self.params = opts
        self.connection_string = get_connection_string(self.name, address, opts)
        self.client = sql.create_engine(self.connection_string)
        self.Session = sessionmaker(bind=self.client)
        
    def setup_connection(self, address):
        pass
    