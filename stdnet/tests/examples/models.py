import time
from datetime import datetime, date

from stdnet import orm

class SimpleModel(orm.StdModel):
    code = orm.SymbolField(unique = True)
    

class Base(orm.StdModel):
    name = orm.SymbolField(unique = True)
    ccy  = orm.SymbolField()
    
    def __str__(self):
        return str(self.name)
    
    class Meta:
        abstract = True
    

class Instrument(Base):
    type = orm.SymbolField()

    
class Fund(Base):
    description = orm.CharField()


class Position(orm.StdModel):
    instrument = orm.ForeignKey(Instrument, related_name = 'positions')
    fund       = orm.ForeignKey(Fund)
    dt         = orm.DateField()
    size       = orm.FloatField()
    price      = orm.FloatField()
    
    def __str__(self):
        return '%s: %s @ %s' % (self.fund,self.instrument,self.dt)


class PortfolioView(orm.StdModel):
    name      = orm.SymbolField()
    portfolio = orm.ForeignKey(Fund)
    
    
class UserDefaultView(orm.StdModel):
    user = orm.SymbolField()
    view = orm.ForeignKey(PortfolioView)
    
    
class DateValue(orm.StdModel):
    "An helper class for adding calendar events"
    
    def __init__(self, dt, value):
        self.dt = dt
        self.value = value
        super(DateValue,self).__init__()
    
    def score(self):
        "implement the score function for sorting in the ordered set"
        return int(1000*time.mktime(self.dt.timetuple()))
    
    
class Calendar(orm.StdModel):
    name   = orm.SymbolField(unique = True)
    data   = orm.SetField(model = DateValue, ordered = True)
    
    def add(self, dt, value):
        event = DateValue(dt,value).save()
        self.data.add(event)

    
class Dictionary(orm.StdModel):
    name = orm.SymbolField(unique = True)
    data = orm.HashField()
    
    
class SimpleList(orm.StdModel):
    names = orm.ListField()


class TestDateModel(orm.StdModel):
    name = orm.SymbolField()
    dt = orm.DateField()
    
    
# Create the model for testing.
class Node(orm.StdModel):
    parent = orm.ForeignKey('self', required = False, related_name = 'children')
    def __init__(self, weight = 1.0, **kwargs):
        super(Node,self).__init__(**kwargs)
        self.weight = weight
    
    def __str__(self):
        return '%s' % self.weight
    

#############################################################
## TWITTER CLONE MODELS

class Post(orm.StdModel):
    dt   = orm.DateTimeField(index = False)
    data = orm.CharField()
    user = orm.ForeignKey("User", index = False)
    
    def __init__(self, data = '', dt = None):
        dt   = dt or datetime.now()
        super(Post,self).__init__(data = data, dt = dt)
    
    
class User(orm.StdModel):
    '''A model for holding information about users'''
    username  = orm.SymbolField(unique = True)
    password  = orm.CharField(required = True)
    updates   = orm.ListField(model = Post)
    following = orm.ManyToManyField(model = 'self', related_name = 'followers')
    
    def __str__(self):
        return self.username
    
    def newupdate(self, data):
        p  = Post(data = data).save()
        #p  = Post(data = data, user = "self").save()
        self.updates.push_front(p)
        return p
    
