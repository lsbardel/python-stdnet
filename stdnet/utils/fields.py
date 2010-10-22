

class ModelFieldPickler(object):
    
    def __init__(self, model):
        self.model = model
        self.get   = model.objects.get
        
    def loads(self, s):
        return self.get(id = s)
    
    def dumps(self, obj):
        return obj.id
    

class listPipeline(object):
    def __init__(self):
        self.clear()
        
    def push_front(self, value):
        self.front.append(value)
        
    def push_back(self, value):
        self.back.append(value)
        
    def clear(self):
        self.back = []
        self.front = []
        
    def __len__(self):
        return len(self.back) + len(self.front)
    

class many2manyPipeline(object):
    def __init__(self):
        self.pipe = {}
    
    def get(self, id):
        s = self.pipe.get(id,None)
        if s is None:
            s = set()
            self.pipe[id] = s
        return s
    __getitem__ = get
    
    def __iter__(self):
        return self.pipe.iteritems()
    
    def clear(self):
        self.pipe.clear()
    