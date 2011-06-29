import threading
from time import time, sleep
from random import random

from stdnet import test
from stdnet.utils import populate
from stdnet.contrib import tasks 


TEST_LEN = 100
names = populate('string',TEST_LEN, min_len = 5, max_len = 10)


def timeit(f, *args, **kwargs):
    t = time()
    v = f(*args,**kwargs)
    t = time() - t
    return v,t


class TestQueue(test.TestCase):
    
    def setUp(self):
        self.orm.register(tasks.Queue)
        self.q = tasks.Queue(name = 'test').save()
        
    def unregister(self):
        self.orm.unregister(tasks.Queue)
        
    def testPutGet(self):
        N = len(names)
        q = self.q
        for name in names:
            q.put(name)
        self.assertEqual(q.qsize(),N)
        for name in names:
            self.assertEqual(name,q.get())
        self.assertTrue(q.empty())
        
    def testSimpleTimeout(self):
        q = self.q
        self.assertTrue(q.empty())
        v,t = timeit(q.get, timeout = 1)
        self.assertEqual(v,None)
        self.assertTrue(t<5)
        v,t = timeit(q.get, block = False)
        self.assertEqual(v,None)
        self.assertTrue(t<1)
        
    def __testTimeout(self):
        
        def putData(q):
            for i in range(10):
                sleep(0.1*random())
                q.put(time())
        
        def getData(q):
            for i in range(10):
                v = q.get(timeout = 5)
                self.assertTrue(v<time())
            
        t1 = threading.Thread(target=putData, args = (self.q,))
        t2 = threading.Thread(target=getData, args = (self.q,))
        
        t1.start()
        t2.start()
        
