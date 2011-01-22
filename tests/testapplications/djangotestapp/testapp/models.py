from django.db import models

from stdnet import orm


class Article(models.Model):
    title = models.CharField(max_length = 200)
    published = models.DateTimeField()
    body = models.TextField()
    
    
class ArticleAndComments(orm.StdModel):
    comments = orm.ListField()
    
    
class Strategy(models.Model):
    name = models.CharField(unique = True, max_length = 20)
    body = models.TextField()
    
class StrategyData(orm.StdModel):
    name     = orm.SymbolField(unique = True)
    data     = orm.HashField()

    
class Environment(ArticleAndComments):
    '''A derived model. To test for manager.'''
    pass