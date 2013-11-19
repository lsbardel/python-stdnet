import time
from datetime import datetime, date

from stdnet import odm


class CustomManager(odm.Manager):

    def small_query(self, **kwargs):
        return self.query(**kwargs).load_only('code', 'group')

    def something(self):
        return "I'm a custom manager"


class SimpleModel(odm.StdModel):
    code = odm.SymbolField(unique=True)
    group = odm.SymbolField(required=False)
    description = odm.CharField()
    somebytes = odm.ByteField()
    object = odm.PickleObjectField(required=False)
    cached_data = odm.ByteField(as_cache=True)
    timestamp = odm.DateTimeField(as_cache=True)
    number = odm.FloatField(required=False)

    manager_class = CustomManager

    def __unicode__(self):
        return self.code


#####################################################################
#    FINANCE APPLICATION
class Base(odm.StdModel):
    name = odm.SymbolField(unique=True)
    ccy = odm.SymbolField()

    def __unicode__(self):
        return self.name

    class Meta:
        abstract = True


class Instrument(Base):
    type = odm.SymbolField()
    description = odm.CharField()


class Instrument2(Base):
    type = odm.SymbolField()

    class Meta:
        ordering = 'id'
        app_label = 'examples2'
        name = 'instrument'


class Fund(Base):
    description = odm.CharField()


class Position(odm.StdModel):
    instrument = odm.ForeignKey(Instrument, related_name='positions')
    fund = odm.ForeignKey(Fund, related_name='positions')
    dt = odm.DateField()
    size = odm.FloatField(default=1)

    def __unicode__(self):
        return '%s: %s @ %s' % (self.fund, self.instrument, self.dt)


class PortfolioView(odm.StdModel):
    name = odm.SymbolField()
    portfolio = odm.ForeignKey(Fund)


class Folder(odm.StdModel):
    name = odm.SymbolField()
    view = odm.ForeignKey(PortfolioView, related_name='folders')
    positions = odm.ManyToManyField(Position, related_name='folders')
    parent = odm.ForeignKey('self', related_name='children', required=False)

    def __unicode__(self):
        return self.name


class UserDefaultView(odm.StdModel):
    user = odm.SymbolField()
    view = odm.ForeignKey(PortfolioView)


class DateValue(odm.StdModel):
    "An helper class for adding calendar events"
    dt = odm.DateField(index=False)
    value = odm.CharField()

    def score(self):
        "implement the score function for sorting in the ordered set"
        return int(1000*time.mktime(self.dt.timetuple()))


class Calendar(odm.StdModel):
    name = odm.SymbolField(unique=True)
    data = odm.SetField(DateValue, ordered=True)

    def add(self, dt, value):
        event = DateValue(dt=dt, value=value).save()
        self.data.add(event)


class Dictionary(odm.StdModel):
    name = odm.SymbolField(unique=True)
    data = odm.HashField()


class SimpleList(odm.StdModel):
    names = odm.ListField()


class SimpleString(odm.StdModel):
    data = odm.StringField()


class TestDateModel(odm.StdModel):
    person = odm.SymbolField()
    name = odm.SymbolField()
    dt = odm.DateField()


class SportAtDate(TestDateModel):

    class Meta:
        ordering = 'dt'


class SportAtDate2(TestDateModel):

    class Meta:
        ordering = '-dt'


class Group(odm.StdModel):
    name = odm.SymbolField()
    description = odm.CharField()


class Person(odm.StdModel):
    name = odm.SymbolField()
    group = odm.ForeignKey(Group)


# A model for testing a recursive foreign key
class Node(odm.StdModel):
    parent = odm.ForeignKey('self', required=False, related_name='children')
    weight = odm.FloatField()

    def __unicode__(self):
        return '%s' % self.weight


class Page(odm.StdModel):
    in_navigation = odm.IntegerField(default=1)


class Collection(odm.StdModel):
    numbers = odm.SetField()
    groups = odm.SetField(model=Group)


#############################################################
## TWITTER CLONE MODELS
class Post(odm.StdModel):
    dt = odm.DateTimeField(index=False, default=datetime.now)
    data = odm.CharField(required=True)
    user = odm.ForeignKey('examples.user', index=False)

    def __unicode__(self):
        return self.data


class User(odm.StdModel):
    '''A model for holding information about users'''
    username = odm.SymbolField(unique=True)
    password = odm.SymbolField(index=False)
    updates = odm.ListField(model=Post)
    following = odm.ManyToManyField(model='self', related_name='followers')

    def __unicode__(self):
        return self.username

    def newupdate(self, message):
        session = self.session
        p = yield session.router.post.new(data=message, user=self)
        yield self.updates.push_front(p)
        yield p


##############################################
class Role(odm.StdModel):
    name = odm.SymbolField(unique=True)
    permissions = odm.JSONField(default=list)

    def __unicode__(self):
        return self.name


class Profile(odm.StdModel):
    name = odm.SymbolField()
    roles = odm.ManyToManyField(model=Role, related_name="profiles")


##############################################
# JSON FIELD
class Statistics(odm.StdModel):
    dt = odm.DateField()
    data = odm.JSONField()


class Statistics3(odm.StdModel):
    name = odm.SymbolField()
    data = odm.JSONField(as_string=False)


class ComplexModel(odm.StdModel):
    name = odm.SymbolField()
    timestamp = odm.DateTimeField(as_cache=True)
    data = odm.JSONField(as_string=False, as_cache=True)


##############################################
# PickleObjectField FIELD
class Environment(odm.StdModel):
    data = odm.PickleObjectField()


##############################################
# Numeric Data

class NumericData(odm.StdModel):
    pv = odm.FloatField()
    vega = odm.FloatField(default=0.0)
    delta = odm.FloatField(default=1.0)
    gamma = odm.FloatField(required=False)
    data = odm.JSONField(as_string=False)
    ok = odm.BooleanField()


class DateData(odm.StdModel):
    dt1 = odm.DateField(required=False)
    dt2 = odm.DateTimeField(default=datetime.now)


#######################################################################
# For testing Foreign Key which is not required range lookup on
# Foreign Keys
class CrossData(odm.StdModel):
    name = odm.SymbolField()
    data = odm.JSONField(as_string=False)
    extra = odm.ForeignKey('self', required=False)


class FeedBase(odm.StdModel):
    name = odm.SymbolField()
    live = odm.ForeignKey(CrossData, required=False)
    prev = odm.ForeignKey(CrossData, required=False)

    class Meta:
        abstract = True


class Feed1(FeedBase):
    pass


class Feed2(FeedBase):
    pass


####################################################
# Custom ID
class Task(odm.StdModel):
    id = odm.SymbolField(primary_key=True)
    name = odm.CharField()
    timestamp = odm.DateTimeField(default=datetime.now)

    class Meta:
        ordering = '-timestamp'

    def clone(self, **kwargs):
        instance = super(Task, self).clone(**kwargs)
        instance.timestamp = None
        return instance


class Parent(odm.StdModel):
    name = odm.SymbolField(primary_key=True)
    timestamp = odm.DateTimeField(default=datetime.now)


class Child(odm.StdModel):
    name = odm.SymbolField()
    parent = odm.ForeignKey(Parent)
    uncles = odm.ManyToManyField(Parent, related_name='nephews')


####################################################
# Composite ID
class WordBook(odm.StdModel):
    id = odm.CompositeIdField('word', 'book')
    word = odm.SymbolField()
    book = odm.SymbolField()

    def __unicode__(self):
        return '%s:%s' % (self.word, self.book)


############################################################################
#   Object Analytics
class ObjectAnalytics(odm.StdModel):
    model_type = odm.ModelField()
    object_id = odm.SymbolField()

    @property
    def object(self):
        if not hasattr(self, '_object'):
            self._object = self.model_type.objects.get(id=self.object_id)
        return self._object


class AnalyticData(odm.StdModel):
    group = odm.ForeignKey(Group)
    object = odm.ForeignKey(ObjectAnalytics, related_name='analytics')
    data = odm.JSONField()
