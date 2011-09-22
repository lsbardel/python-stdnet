from stdnet import orm
from stdnet.apps.grid import Queue


class Task(orm.StdModel):
    name = orm.CharField()
    
    
class TaskQueue(Queue):
    queue = orm.ListField(Task)