from stdnet import orm


class TaggedItem(orm.StdModel):
    tag = orm.SymbolField()
    model_type = orm.ModelField()
    object_id = orm.SymbolField()
    
    def __unicode__(self):
        return self.tag
    
    @property
    def object(self):
        return self.model_type.objects.get(id = self.object_id)
        
