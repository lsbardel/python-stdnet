from datetime import datetime
from django.core.management import setup_environ
import settings
setup_environ(settings)
from django.conf import settings

#from django import test
from stdnet import orm, test
from stdnet.contrib.djangolink import link_models, LinkedManager
from testmodel.models import Article, ArticleAndComments, Environment


__all__ = ['DjangoStdNetLinkTest']


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
    
    def __testCreate(self):
        a = Article(title = 'test article', published = datetime.now(), body = 'bla bla bla')
        a.save()
        
    def tearDown(self):
        orm.clearall()
        self.unregister()