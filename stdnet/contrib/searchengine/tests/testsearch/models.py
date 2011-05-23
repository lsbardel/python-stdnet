from stdnet import orm

class Item(orm.StdModel):
    name = orm.SymbolField()
    content = orm.CharField()
    counter = orm.IntegerField()