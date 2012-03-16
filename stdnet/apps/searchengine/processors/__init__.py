from .ignore import STOP_WORDS, PUNCTUATION_CHARS
from .metaphone import dm as double_metaphone
from .porter import PorterStemmer


class stopwords:
    
    def __init__(self, stp = None):
        self.stp = stp if stp is not None else STOP_WORDS
        
    def __call__(self, words):
        stp = self.stp
        for word in words:
            if word not in stp:
                yield word
                
                
def metaphone_processor(words):
    '''Double metaphone word processor.'''
    for word in words:
        for w in double_metaphone(word):
            if w:
                w = w.strip()
                if w:
                    yield w
                    

def tolerant_metaphone_processor(words):
    '''Double metaphone word processor slightly modified so that when no
words are returned by the algorithm, the original word is returned.'''
    for word in words:
        r = 0
        for w in double_metaphone(word):
            if w:
                w = w.strip()
                if w:
                    r += 1
                    yield w
        if not r:
            yield word
                
                
def stemming_processor(words):
    '''Porter Stemmer word processor'''
    stem = PorterStemmer().stem
    for word in words:
        word = stem(word, 0, len(word)-1)
        yield word