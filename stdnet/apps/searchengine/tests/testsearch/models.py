from stdnet import orm


class RelatedItem(orm.StdModel):
    name = orm.SymbolField()
    
    
class Item(orm.StdModel):
    name = orm.SymbolField()
    content = orm.CharField()
    counter = orm.IntegerField()
    related = orm.ForeignKey(RelatedItem,required=False)