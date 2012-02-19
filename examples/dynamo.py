from stdnet import orm


class Base(orm.StdModel):
    name = orm.SymbolField(primary_key = True)
    ccy  = orm.SymbolField()
    
    def __unicode__(self):
        return self.name
    
    class Meta:
        abstract = True
    

class Instrument(Base):
    type = orm.SymbolField()
    description = orm.CharField()