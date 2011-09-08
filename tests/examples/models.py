import time
from datetime import datetime, date

from stdnet import orm
from stdnet.orm import query


class CustomManager(query.Manager):
    
    def something(self):
        return "I'm a custom manager"


class SimpleModel(orm.StdModel):
    code = orm.SymbolField(unique = True)
    group = orm.SymbolField(required = False)
    description = orm.CharField()
    somebytes = orm.ByteField()
    object = orm.PickleObjectField(required = False)
    
    objects = CustomManager()
    
    def __unicode__(self):
        return self.code
    
    
    

#####################################################################
#    FINANCE APPLICATION
class Base(orm.StdModel):
    name = orm.SymbolField(unique = True)
    ccy  = orm.SymbolField()
    
    def __unicode__(self):
        return self.name
    
    class Meta:
        abstract = True
    

class Instrument(Base):
    type = orm.SymbolField()


class Instrument2(Base):
    type = orm.SymbolField()

    class Meta:
        ordering = 'id'
        
    
class Fund(Base):
    description = orm.CharField()


class Position(orm.StdModel):
    instrument = orm.ForeignKey(Instrument, related_name = 'positions')
    fund       = orm.ForeignKey(Fund)
    dt         = orm.DateField()
    size       = orm.FloatField(default = 1)
    
    def __unicode__(self):
        return '%s: %s @ %s' % (self.fund,self.instrument,self.dt)
    
    
class PortfolioView(orm.StdModel):
    name      = orm.SymbolField()
    portfolio = orm.ForeignKey(Fund)
    
    
class Folder(orm.StdModel):
    name      = orm.SymbolField()
    view      = orm.ForeignKey(PortfolioView, related_name = 'folders')
    positions = orm.ManyToManyField(Position, related_name = 'folders')
    parent    = orm.ForeignKey('self',related_name = 'children',required=False)

    def __unicode__(self):
        return self.name


class UserDefaultView(orm.StdModel):
    user = orm.SymbolField()
    view = orm.ForeignKey(PortfolioView)
    
    
class DateValue(orm.StdModel):
    "An helper class for adding calendar events"
    dt = orm.DateField(index = False)
    value = orm.CharField()
    
    @classmethod
    def score(cls, instance):
        "implement the score function for sorting in the ordered set"
        return int(1000*time.mktime(instance.dt.timetuple()))
    
    
class Calendar(orm.StdModel):
    name   = orm.SymbolField(unique = True)
    data   = orm.SetField(DateValue, ordered = True,
                          scorefun = DateValue.score)
    
    def add(self, dt, value):
        event = DateValue(dt = dt,value = value).save()
        self.data.add(event)

    
class Dictionary(orm.StdModel):
    name = orm.SymbolField(unique = True)
    data = orm.HashField()
    
    
class SimpleList(orm.StdModel):
    names = orm.ListField()
    
    
class TestDateModel(orm.StdModel):
    person = orm.SymbolField()
    name = orm.SymbolField()
    dt = orm.DateField()


class SportAtDate(TestDateModel):
    
    class Meta:
        ordering = 'dt'
    

class SportAtDate2(TestDateModel):
    
    class Meta:
        ordering = '-dt'
    

class Group(orm.StdModel):
    name = orm.SymbolField()
    
    
class Person(orm.StdModel):
    name = orm.SymbolField()
    group = orm.ForeignKey(Group)

    
# A model for testing a recursive foreign key
class Node(orm.StdModel):
    parent = orm.ForeignKey('self', required = False, related_name = 'children')
    weight = orm.FloatField()
    
    def __unicode__(self):
        return '%s' % self.weight
    
    
class Page(orm.StdModel):
    in_navigation = orm.IntegerField(default = 1)
    
    

#############################################################
## TWITTER CLONE MODELS

class Post(orm.StdModel):
    dt   = orm.DateTimeField(index = False)
    data = orm.CharField(required = True)
    user = orm.ForeignKey("User", index = False)
    
    def __unicode__(self):
        return self.data
    
    
class User(orm.StdModel):
    '''A model for holding information about users'''
    username  = orm.SymbolField(unique = True)
    password  = orm.SymbolField(index = False)
    updates   = orm.ListField(model = Post)
    following = orm.ManyToManyField(model = 'self', related_name = 'followers')
    
    def __unicode__(self):
        return self.username
    
    def newupdate(self, data):
        p  = Post(data = data, user = self, dt = datetime.now()).save()
        self.updates.push_front(p)
        return p
    


##############################################
class Role(orm.StdModel):
    name = orm.SymbolField()


class Profile(orm.StdModel):
    roles = orm.ManyToManyField(model=Role,
                                related_name="profiles")


##############################################
# JSON FIELD

class Statistics(orm.StdModel):
    dt = orm.DateField()
    data = orm.JSONField()
    
    
class Statistics2(orm.StdModel):
    dt = orm.DateField()
    data = orm.JSONField(sep = True)
    
    
class Statistics3(orm.StdModel):
    name = orm.SymbolField()
    data = orm.JSONField(as_string = False)
    
    
    
##############################################
# PickleObjectField FIELD

class Environment(orm.StdModel):
    data = orm.PickleObjectField()
    

##############################################
# Numeric Data

class NumericData(orm.StdModel):
    pv = orm.FloatField()
    vega = orm.FloatField(default = 0.)
    delta = orm.FloatField(default = 1.0)
    gamma = orm.FloatField(required = False)
    ok = orm.BooleanField()


class DateData(orm.StdModel):
    dt1 = orm.DateField(required = False)
    dt2 = orm.DateTimeField(default = datetime.now)
    