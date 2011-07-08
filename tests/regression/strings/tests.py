import time
import random

import stdnet
from stdnet.test import TestCase

cache = stdnet.getdb()


class TestString(TestCase):
    
    def testSetGet(self):
        cache.set('test',1)
        self.assertEqual(cache.get('test'),b'1')
        cache.set('test2','ciao',1)
        self.assertEqual(cache.get('test2'),b'ciao')
        time.sleep(2)
        self.assertEqual(cache.get('test2'),None)