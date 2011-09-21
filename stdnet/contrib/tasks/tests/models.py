from stdnet import orm
from stdnet.contrib.tasks import Queue


class Task(orm.StdModel):
    name = orm.CharField()
    
    
class TaskQueue(Queue):
    queue = orm.ListField(Task)