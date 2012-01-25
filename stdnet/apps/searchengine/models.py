'''\
Search Engine and Tagging models. Just two of them, one for storing Words and
one for linking other objects to Words.
'''
from inspect import isclass

from stdnet import orm
from stdnet.utils import range, to_string


class WordItemManager(orm.Manager):
    
    def for_model(self, model):
        q = self.query()
        if not isclass(model):
            return q.filter(model_type = model.__class__,
                            object_id = model.id)
        else:
            return q.filter(model_type = model)


class Word(orm.StdModel):
    '''Model which hold a word as primary key'''
    id = orm.SymbolField(primary_key = True)
    tag = orm.BooleanField(default = False)
    # denormalised fields for frequency
    frequency = orm.IntegerField(default = 0)
    model_frequency = orm.HashField()
    
    def __unicode__(self):
        return self.id
    
    def update_frequency(self):
        f = 0
        mf = {}
        for item in self.items().all():
            m = item.model_type
            if m in mf:
                mf[m] += 1
            else:
                mf[m] = 1
            f += 1
        self.model_frequency.delete()
        self.model_frequency.update(mf)
        self.frequency = f
        self.save()


class WordItem(orm.StdModel):
    '''A model for associating :class:`Word` instances with general
:class:`stdnet.orm.StdModel` instances.'''
    word = orm.ForeignKey(Word, related_name = 'items')
    '''tag instance'''
    model_type = orm.ModelField()
    '''Model type'''
    object_id = orm.SymbolField()
    '''Model instance id'''
    count = orm.IntegerField(index = False, default = 1)
    
    def __unicode__(self):
        return self.word.__unicode__()
    
    objects = WordItemManager()
    
    @property
    def object(self):
        '''Instance of :attr:`model_type` with id :attr:`object_id`.'''
        if not hasattr(self,'_object'):
            self._object = self.model_type.objects.get(id = self.object_id)
        return self._object
    

class Tag(Word):
    pass


class TagItem():
    '''A model for associating :class:`Word` instances with general
:class:`stdnet.orm.StdModel` instances.'''
    tag = orm.ForeignKey(Tag, related_name = 'items')
    '''tag instance'''
    model_type = orm.ModelField()
    '''Model type'''
    object_id = orm.SymbolField()
    '''Model instance id'''
    
    def __unicode__(self):
        return self.word.__unicode__()
    
    @property
    def object(self):
        '''Instance of :attr:`model_type` with id :attr:`object_id`.'''
        if not hasattr(self,'_object'):
            self._object = self.model_type.objects.get(id = self.object_id)
        return self._object
    
