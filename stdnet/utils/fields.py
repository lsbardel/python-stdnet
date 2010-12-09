
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
    
