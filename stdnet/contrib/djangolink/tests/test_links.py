from datetime import datetime

from stdnet import orm, test
from stdnet.contrib.djangolink import link_models, LinkedManager
from djangotestapp.testapp.models import Article, ArticleAndComments, Environment


class DjangoStdNetLinkTest(test.TestCase):
    tags = ['django']
    
    def setUp(self):
        orm.register(ArticleAndComments)
        orm.register(Environment)
        link_models(Article,ArticleAndComments)
        
    def unregister(self):
        orm.unregister(ArticleAndComments)
    
    def testLinked(self):
        self.assertEqual(ArticleAndComments._meta.linked,Article)
        self.assertEqual(Article._meta.linked,ArticleAndComments)
        self.assertTrue(isinstance(ArticleAndComments.objects,LinkedManager))
        
    def testDerivedManager(self):
        self.assertFalse(isinstance(Environment.objects,LinkedManager))
    
    def testCreate(self):
        a = Article(title = 'test article', published = datetime.now(), body = 'bla bla bla')
        a.save()
        ac = ArticleAndComments.objects.get(id = a.id)
        self.assertEqual(a,ac.djobject)
        
    def testDelete(self):
        self.testCreate()
        self.assertTrue(ArticleAndComments.objects.all().count())
        Article.objects.all().delete()
        self.assertFalse(ArticleAndComments.objects.all().count())
        