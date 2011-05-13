from stdnet import orm
from stdnet.utils import range

from .ignore import STOP_WORDS, MIN_WORD_LENGTH

class ItemPointer(orm.StdModel):
    '''tag instance'''
    model_type = orm.ModelField()
    '''Model type'''
    object_id = orm.SymbolField()
    '''Model instance id'''
    
    class Meta:
        abstract = True
        
    def __unicode__(self):
        return self.tag.__unicode__()
    
    @property
    def object(self):
        '''Instance of :attr:`model_type` with id :attr:`object_id`.'''
        if not hasattr(self,'_object'):
            self._object = self.model_type.objects.get(id = self.object_id)
        return self._object


class Word(orm.StdModel):
    '''Model which hold a word as primary key'''
    id = orm.SymbolField(primary_key = True)
    tag = orm.BooleanField(default = False)
    # denormalised fields for frequency
    frequency = orm.IntegerField()
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


class Tag(orm.StdModel):
    id = orm.SymbolField(primary_key = True)
    '''The tag name'''

    def __unicode__(self):
        return self.id


class TaggedItem(ItemPointer):
    '''A model for associating :class:`Tag` instances with general :class:`stdnet.orm.StdModel`
instances.'''
    tag = orm.ForeignKey(Tag)
    
    def __unicode__(self):
        return self.tag.__unicode__()
    

class WordItem(ItemPointer):
    '''A model for associating :class:`Word` instances with general
:class:`stdnet.orm.StdModel` instances.'''
    word = orm.ForeignKey(Word, related_name = 'items')
    
    def __unicode__(self):
        return self.word.__unicode__()

