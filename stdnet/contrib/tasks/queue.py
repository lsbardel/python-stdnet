from multiprocessing.queues import Empty

from stdnet import orm


class Queue(orm.StdModel):
    '''E Redis queue object which exposes exactly the same functionalities
as a python multiprocessing Queue. To use it::

    >>> q = Queue('test-queue').save()
    >>> q.id
    1
    >>> q.put('hello')
    >>> q.put('world')
'''
    name = orm.CharField(required = False)
    queue = orm.ListField()
    
    def __unicode__(self):
        return self.name
    
    def put(self, elem):
        '''Put item into the queue.'''
        q = self.queue
        q.push_front(elem)
        q.save()
        
    def get(self, block = True, timeout = None):
        '''Remove and return an item from the queue.
If optional args ``block`` is ``True`` (the default)
and ``timeout`` is ``0`` (the default), block if necessary until
an item is available.
If timeout is a positive number, it blocks at most ``timeout`` seconds.

It raises  and `multiprocessing.queues.Empty` exception
if no item was available.'''
        if block:
            timeout = max(1,int(round(timeout))) if timeout else 0
            ret = self.queue.block_pop_back(timeout)
        else:
            ret = self.queue.pop_back()
        if ret is None:
            raise Empty
        return ret
        
    def qsize(self):
        '''Return the approximate size of the queue.'''
        return self.queue.size()
    
    def empty(self):
        '''Return ``True`` if the queue is empty, ``False`` otherwise.'''
        return not self.queue.size()
    
