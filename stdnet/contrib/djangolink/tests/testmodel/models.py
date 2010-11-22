from django.db import models

from stdnet import orm


class Article(models.Model):
    title = models.CharField(max_length = 200)
    published = models.DateTimeField()
    body = models.TextField()
    
    
class ArticleAndComments(orm.StdModel):
    comments = orm.ListField()

    
class Environment(ArticleAndComments):
    '''A derived model. To test for manager.'''
    pass