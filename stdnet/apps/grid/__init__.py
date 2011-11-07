from .models import *


def queue(name):
    try:
        return Queue.objects.get(id = name)
    except Queue.DoesNotExist:
        return Queue(id = name).save()

class TaskQueueManager(object):
    
    def opensession(self,
                     common_data = None,
                     task_callback = None,
                     expiry = None):
        common_data = common_data
        session = GridSession.objects.create(expiry = expiry,
                                             common_data = common_data)
        return session.id