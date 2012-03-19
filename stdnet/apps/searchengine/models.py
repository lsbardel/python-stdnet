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


class WordItem(orm.StdModel):
    '''A model for associating :class:`Word` instances with general
:class:`stdnet.orm.StdModel` instances.'''
    id = orm.CompositeIdField('word','model_type','object_id')
    #
    word = orm.SymbolField()
    '''tag instance'''
    model_type = orm.ModelField()
    '''Model type'''
    object_id = orm.SymbolField()
    
    def __unicode__(self):
        return self.word
    
    objects = WordItemManager()
    
    class Meta:
        ordering = -orm.autoincrement()
    
    @property
    def object(self):
        '''Instance of :attr:`model_type` with id :attr:`object_id`.'''
        if not hasattr(self,'_object'):
            self._object = self.model_type.objects.get(id = self.object_id)
        return self._object
    
    
