'''\
Search Engine and Tagging models. Just two of them, one for storing Words and
one for linking other objects to Words.
'''
from stdnet import orm


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


class WordItem(orm.StdModel):
    '''A model for associating :class:`Word` instances with general
:class:`stdnet.orm.StdModel` instances.'''
    word = orm.ForeignKey(Word, related_name = 'items')
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
    

