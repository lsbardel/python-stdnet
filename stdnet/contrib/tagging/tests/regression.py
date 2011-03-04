from random import randint

from stdnet import test
from stdnet.utils import populate
from stdnet.contrib import tagging 

from .models import User, Issue

Tag = tagging.Tag
TaggedItem = tagging.TaggedItem

TAG_LEN = 10
ISSUE_LEN = 100
MAX_TAGS  = 5
tag_names = populate('string',TAG_LEN, min_len = 5, max_len = 10)
issue_des = populate('string',ISSUE_LEN, min_len = 30, max_len = 100)


def random_tag():
    i = randint(0,len(tag_names)-1)
    return tag_names[i]


class TestTags(test.TestCase):
    
    def setUp(self):
        self.orm.register(User)
        self.orm.register(Issue)
        self.orm.register(Tag)
        self.orm.register(TaggedItem)
        #self.orm.clearall()
        self.user = User(username = 'pinco', password = 'pinco').save()
        
    def make(self):
        user = self.user
        for des in issue_des:
            Issue(description = des, user = user).save(commit=False)
        Issue.commit()
        for issue in Issue.objects.all():
            for i in range(0,randint(0,MAX_TAGS)):
                tagging.addtag(issue,random_tag())
            
        
    def testAddTag(self):
        obj = Issue(description = 'just a test', user = self.user).save()
        item = tagging.addtag(obj,'ciao')
        self.assertEqual(obj.id,item.object_id)
        self.assertEqual(Issue,item.model_type)
        self.assertEqual(item.object,obj)
        self.assertEqual(tagging.Tag.objects.all().count(),1)
        tagging.addtag(obj,'ciao')
        self.assertEqual(tagging.Tag.objects.all().count(),1)
        self.assertEqual(tagging.TaggedItem.objects.all().count(),1)
        tagging.addtag(obj,'baa')
        self.assertEqual(tagging.Tag.objects.all().count(),2)
        self.assertEqual(tagging.TaggedItem.objects.all().count(),2)
        
    def testForModel(self):
        self.make()
        tags = tagging.formodels(Issue)
        alltags = Tag.objects.all()
        self.assertEqual(len(tags),alltags.count())
        for tag in alltags:
            n = tags[tag.name]
            self.assertTrue(n)
            tagged = TaggedItem.objects.filter(tag = tag, model_type = Issue)
            self.assertEqual(n,tagged.count())
        
