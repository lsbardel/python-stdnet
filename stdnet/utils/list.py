
class Node:
    def __init__(self, value):
        self.value = value
        self.next  = None
        self.prev  = None
        
class List(object):

    def __init__(self):
        self.begin = None
        self.end   = None
    
    def append(self, value):
        end = self.end
        if not end:
            end = Node(value)
            self.begin = end
        else:
            end = Node(value,prev=end)
        self.end = end
