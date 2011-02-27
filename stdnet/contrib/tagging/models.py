from stdnet import orm


class Tag(orm.StdModel):
    name = orm.SymbolField(unique = True)
    '''The tag name'''

    def __unicode__(self):
        return self.name
    

class TaggedItem(orm.StdModel):
    '''A model for associating :class:`Tag` instances with general :class:`stdnet.orm.StdModel`
instances.'''
    tag = orm.ForeignKey(Tag)
    '''tag instance'''
    model_type = orm.ModelField()
    '''Model type'''
    object_id = orm.SymbolField()
    '''Model instance id'''
    
    def __unicode__(self):
        return self.tag.__unicode__()
    
    @property
    def object(self):
        '''Instance of :attr:`model_type` with id :attr:`object_id`.'''
        if not hasattr(self,'_object'):
            self._object = self.model_type.objects.get(id = self.object_id)
        return self._object
        
