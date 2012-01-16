from stdnet import orm


class Element(orm.StdModel):
    name = orm.SymbolField()


class CompositeElement(orm.StdModel):
    weight = orm.FloatField()
    

class Composite(orm.StdModel):
    name = orm.SymbolField()
    elements = orm.ManyToManyField(Element,
                                   through = CompositeElement,
                                   related_name = 'composites')