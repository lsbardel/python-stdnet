from stdnet import test
from stdnet.contrib import tagging 

from .models import User, Issue


class TestTags(test.TestCase):
    
    def setUp(self):
        self.orm.register(User)
        self.orm.register(Issue)
        self.orm.register(tagging.TaggedItem)
        #self.orm.clearall()
        self.user = User(username = 'pinco', password = 'pinco').save()
        
    def testAddTag(self):
        obj = Issue(description = 'just a test', user = self.user).save()
        item = tagging.addtag(obj,'ciao')
        self.assertEqual(obj.id,item.object_id)
        self.assertEqual(Issue,item.model_type)
        self.assertEqual(item.object,obj)
        
