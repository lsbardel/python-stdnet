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
            return q.filter(model_type=model.__class__, object_id=model.id)
        else:
            return q.filter(model_type=model)


class WordItem(odm.StdModel):
    '''A model for associating a word with general
:class:`stdnet.odm.StdModel` instance.'''
    id = odm.CompositeIdField('word', 'model_type', 'object_id')
    word = odm.SymbolField()
    model_type = odm.ModelField()
    object_id = odm.SymbolField()

    def __unicode__(self):
        return self.word

    manager_class = WordItemManager

    class Meta:
        ordering = -odm.autoincrement()

    def object(self, session):
        '''Instance of :attr:`model_type` with id :attr:`object_id`.'''
        if not hasattr(self, '_object'):
            pkname = self.model_type._meta.pkname()
            query = session.query(self.model_type).filter(**{pkname: self.object_id})
            return query.items(callback=self.__set_object)
        else:
            return self._object

    def __set_object(self, items):
        try:
            self._object = self.get_unique_instance(items)
        except self.DoesNotExist:
            self._object = None
        return self._object
        