# Original from
# http://code.activestate.com/recipes/576930-efficient-running-median-using-an-indexable-skipli/
#
from random import random
from math import log, ceil
from collections import deque
from itertools import islice

__all__ = ['skiplist']

class Node(object):
    __slots__ = ('score', 'value', 'next', 'width')
    def __init__(self, score, value, next, width):
        self.score, self.value, self.next, self.width =\
                         score, value, next, width

class End(object):
    'Sentinel object that always compares greater than another object'
    def __cmp__(self, other):
        return 1
    def __ge__(self, other):
        return 1
    def __gt__(self, other):
        return 1
    def __lt__(self, other):
        return 0
    def __eq__(self, other):
        return 0
    def __le__(self, other):
        return 0

# Singleton terminator node
NIL = Node(End(), None, [], [])


class skiplist(object):
    '''Sorted collection supporting O(log n) insertion, removal,
and lookup by rank.'''

    def __init__(self, expected_size=1000000):
        self.maxlevels = int(1 + log(expected_size, 2))
        self.clear()
        
    def __repr__(self):
        return list(self).__repr__()
    
    def __str__(self):
        return self.__repr__()
    
    def __len__(self):
        return self.size

    def __getitem__(self, i):
        node = self.head
        i += 1
        for level in reversed(range(self.maxlevels)):
            while node.width[level] <= i:
                i -= node.width[level]
                node = node.next[level]
        return node.value

    def clear(self):
        self.size = 0
        self.head = Node('HEAD', None, [NIL]*self.maxlevels, [1]*self.maxlevels)
        
    def insert(self, score, value):
        # find first node on each level where node.next[levels].score > score
        chain = [None] * self.maxlevels
        steps_at_level = [0] * self.maxlevels
        node = self.head
        for level in reversed(range(self.maxlevels)):
            while node.next[level].score <= score:
                steps_at_level[level] += node.width[level]
                node = node.next[level]
            chain[level] = node

        # insert a link to the newnode at each level
        d = min(self.maxlevels, 1 - int(log(random(), 2.0)))
        newnode = Node(score, value, [None]*d, [None]*d)
        steps = 0
        for level in range(d):
            prevnode = chain[level]
            newnode.next[level] = prevnode.next[level]
            prevnode.next[level] = newnode
            newnode.width[level] = prevnode.width[level] - steps
            prevnode.width[level] = steps + 1
            steps += steps_at_level[level]
        for level in range(d, self.maxlevels):
            chain[level].width[level] += 1
        self.size += 1

    def update(self, values):
        insert = self.insert
        for score,value in values:
            insert(score,value)
        
    def remove(self, score):
        # find first node on each level where node.next[levels].score >= score
        chain = [None] * self.maxlevels
        node = self.head
        for level in reversed(range(self.maxlevels)):
            while node.next[level].score < score:
                node = node.next[level]
            chain[level] = node
        if score != chain[0].next[0].score:
            raise KeyError('Not Found')

        # remove one link at each level
        d = len(chain[0].next[0].next)
        for level in range(d):
            prevnode = chain[level]
            prevnode.width[level] += prevnode.next[level].width[level] - 1
            prevnode.next[level] = prevnode.next[level].next[level]
        for level in range(d, self.maxlevels):
            chain[level].width[level] -= 1
        self.size -= 1

    def __iter__(self):
        'Iterate over values in sorted order'
        node = self.head.next[0]
        while node is not NIL:
            yield node.score,node.value
            node = node.next[0]

    def flat(self):
        node = self.head.next[0]
        while node is not NIL:
            yield node.score
            yield node.value
            node = node.next[0]