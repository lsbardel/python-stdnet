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
    
    def __unicode__(self):
        return self.id
    
    class Meta:
        ordering = -orm.autoincrement()


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
    
    class Meta:
        ordering = -orm.autoincrement()
        unique_together = ('word', 'model_type', 'object_id')
    
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
    
