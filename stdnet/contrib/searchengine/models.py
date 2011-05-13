import re

from stdnet import orm
from stdnet.utils import range

from .ignore import STOP_WORDS, MIN_WORD_LENGTH
    

class Word(orm.StdModel):
    '''Model which hold a word as primary key'''
    id = orm.SymbolField(primary_key = True)
    # denormalised fields for frequency
    frequency = orm.IntegerField()
    model_frequency = orm.HashField()
    
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


class WorldItem(orm.StdModel):
    '''A model for associating :class:`World` instances with general :class:`stdnet.orm.StdModel`
instances.'''
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
    


class FullTextIndex(object):
    """A class to provide full-text indexing functionality using StdNet. Adapted from
    
https://gist.github.com/389875
"""    
    def __init__(self):
        self.punctuation_regex = re.compile(r"[%s]" % re.escape(PUNCTUATION_CHARS))
    
    def get_words_from_text(self, text):
        """A generator of words to index from the given text"""
        if not text:
            return []
        
        text = self.punctuation_regex.sub(" ", text)
        
        for word in text.split():
            if len(world) >= MIN_WORD_LENGTH:
                world = world.lower()
                if world not in STOP_WORDS:
                    yield world
        
    def index_item(self, item):
        """Extract content from the given item and add it to the index"""
        # TODO: Added item users to index
        words = self.get_words_from_text(item.subject)
        words += self.get_words_from_text(item.body)
        words += self.get_words_from_text(item.milestone.name)
        words += self.get_words_from_text(item.type_name)
        words += self.get_words_from_text(" ".join(item.tags))
        
        metaphones = self.get_metaphones(words)
        
        for metaphone in metaphones:
            self._link_item_and_metaphone(item, metaphone)
        
    
    def index_item_content(self, item, content):
        """Index a specific bit of item content"""
        words = self.get_words_from_text(content)
        metaphones = self.get_metaphones(words)
        
        for metaphone in metaphones:
            self._link_item_and_metaphone(item, metaphone)
        
    
    def _link_item_and_metaphone(self, item, metaphone):
        # Add the item to the metaphone key
        redis_key = REDIS_KEY_METAPHONE % {"project_id": item.project_id, "metaphone": metaphone}
        redis.sadd(redis_key, item.item_id)
        
        # Make sure we record that this project contains this metaphone
        redis_key = REDIS_KEY_METAPHONES % {"project_id": item.project_id}
        redis.sadd(redis_key, metaphone)
    
    def get_metaphones(self, words):
        """Get the metaphones for a given list of words"""
        metaphones = set()
        for word in words:
            metaphone = double_metaphone(unicode(word))
            
            metaphones.add(metaphone[0].strip())
            if(metaphone[1]):
                metaphones.add(metaphone[1].strip())
        return metaphones
    
    def reindex_project(self, project_id):
        """Reindex an entire project, removing the existing index for the project"""
        
        # Remove all the existing index data
        redis_key = REDIS_KEY_METAPHONES % {"project_id": project_id}
        project_metaphones = redis.smembers(redis_key)
        if project_metaphones is None:
            project_metaphones = []
        
        redis.delete(redis_key)
        
        for project_metaphone in project_metaphones:
            redis.delete(REDIS_KEY_METAPHONE % {"project_id": project_id, "metaphone": project_metaphone})
        
        # Now index each item
        project = models.Project(project_id)
        for item in project.items:
            self.index_item(item)
        
        return True
