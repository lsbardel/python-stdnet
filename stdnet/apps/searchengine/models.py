'''\
Search Engine and Tagging models. Just two of them, one for storing Words and
one for linking other objects to Words.
'''
from inspect import isclass

from stdnet import odm


class WordItemManager(odm.Manager):

    def for_model(self, model):
        q = self.query()
        if not isclass(model):
            return q.filter(model_type = model.__class__,
                            object_id = model.id)
        else:
            return q.filter(model_type = model)


class WordItem(odm.StdModel):
    '''A model for associating a word with general
:class:`stdnet.odm.StdModel` instance.'''
    id = odm.CompositeIdField('word', 'model_type', 'object_id')
    word = odm.SymbolField()
    model_type = odm.ModelField()
    object_id = odm.SymbolField()

    def __unicode__(self):
        return self.word

    objects = WordItemManager()

    class Meta:
        ordering = -odm.autoincrement()

    @property
    def object(self):
        '''Instance of :attr:`model_type` with id :attr:`object_id`.'''
        if not hasattr(self,'_object'):
            self._object = self.model_type.objects.get(id = self.object_id)
        return self._object


