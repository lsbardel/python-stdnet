'''\
Search Engine and Tagging models. Just two of them, one for storing Words and
one for linking other objects to Words.
'''
from stdnet import orm
from stdnet.utils import range, to_string


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
    
    
class AutoComplete(orm.StdModel):
    '''A model which should be used as signletone::
    
    AutoComplete.search(string_value, maxelem = 50)
    '''
    endchar = '*'
    '''Character appended to an actual world to distinguish it from
autocomplete helpers. Default ``2``.'''
    minlen = 2
    '''Minimum length of text to start the search. Default ``2``.'''
    
    id = orm.SymbolField(primary_key = True)
    data = orm.SetField(ordered = True,
                        pickler = False,
                        scorefun = lambda x : 1)
    
    @classmethod
    def me(cls, id = 'en'):
        try:
            return cls.objects.get(id = id)
        except cls.DoesNotExist:
            return cls(id = id).save()
    
    def search(self, value, maxelem = -1):
        '''Search for ``value`` in the ordered dataset and
return an iterator of suggested words'''
        M = len(value)
        if M < self.minlen:
            raise StopIteration
        rank = self.data.rank(value)
        if rank is not None:
            if maxelem > 0:
                end = rank + maxelem
            elif maxelem < 0:
                end = maxelem
            else:
                end = rank
            elems = self.data.range(rank, end)
            echar = self.endchar
            N = len(echar)
            for elem in elems:
                elem = to_string(elem)
                if elem[:M] != value:
                    raise StopIteration
                if elem.endswith(echar):
                    yield elem[:-N]
    
    def add(self, word):
        '''Add a word to the dataset'''
        dataset = self.data
        we = word+self.endchar
        if word in dataset:
            dataset.add(we)
            dataset.save()
        elif we not in dataset:
            for idx in range(self.minlen,len(word)):
                dataset.add(word[:idx])
            dataset.add(we)
            dataset.save()

    def extend(self, words):
        add = self.data.add
        for word in words:
            we = word+self.endchar
            add(we)
            for idx in range(self.minlen,len(word)):
                add(word[:idx])
        self.data.save()
