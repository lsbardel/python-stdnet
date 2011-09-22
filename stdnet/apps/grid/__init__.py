from .models import *


class TaskQueueManager(object):
    
    def opensession(self,
                     common_data = None,
                     task_callback = None,
                     expiry = None):
        common_data = common_data
        session = GridSession.objects.create(expiry = expiry,
                                             common_data = common_data)
        return session.id