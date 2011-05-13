

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
    
    @classmethod    
    def search(self, value, maxelem = -1):
        '''Search for ``value`` in the ordered dataset and
return an iterator of suggested words'''
        M = len(value)
        if M < self.minlen:
            raise StopIteration
        rank = self.data.rank(value)
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
            if elem[M:] != value:
                raise StopIteration
            if elem.endswith(echar):
                yield elem[:-N]
    
    def add(self, word):
        if word not in ignore_words:
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
            


autocomplete = AutoComplete.me

