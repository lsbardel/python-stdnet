from stdnet.test import TestCase

from stdnet.contrib.sessions.models import Session



class TestSessions(TestCase):
    
    def setUp(self):
        self.orm.register(Session)
        
    def unregister(self):
        self.orm.unregister(Session)
         
    def testCreate(self):
        s = Session.objects.create()
        self.assertTrue(s.id)