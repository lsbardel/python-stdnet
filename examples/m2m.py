from stdnet import odm


class Element(odm.StdModel):
    name = odm.SymbolField()


class CompositeElement(odm.StdModel):
    weight = odm.FloatField()


class Composite(odm.StdModel):
    name = odm.SymbolField()
    elements = odm.ManyToManyField(Element, through=CompositeElement,
                                   related_name='composites')