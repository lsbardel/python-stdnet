from stdnet import odm


class RelatedItem(odm.StdModel):
    name = odm.SymbolField()


class Item(odm.StdModel):
    name = odm.SymbolField()
    content = odm.CharField()
    counter = odm.IntegerField()
    related = odm.ForeignKey(RelatedItem, required=False)
    secret = odm.CharField(hidden=True)
