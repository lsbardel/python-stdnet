from uuid import uuid4
from datetime import datetime, timedelta

from stdnet import orm


class GridSessionManager(orm.Manager):
    
    def create(self, expiry = None, common_data = None):
        '''Create a new grid session with *expiry* in seconds or ``None``.'''
        if expiry:
            expiry = datetime.now() + timedelta(seconds = expiry)
        return self.model(id = self.new_session_id(),
                          expiry = expiry,
                          common_data = common_data).save()
    
    def new_session_id(self):
        "Returns session key that isn't being used."
        while True:
            id = str(uuid4())[:8]
            if not self.exists(id):
                return id

    def exists(self, id):
        try:
            self.get(id = id)
        except self.model.DoesNotExist:
            return False
        return True