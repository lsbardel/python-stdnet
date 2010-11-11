from django.db import models

from stdnet import orm


class DataId(models.Model):
    name = models.CharField(unique = True)
    description = models.TextField(blank = True)
    
    
class Data(orm.StdModel):
    data = orm.ListField()