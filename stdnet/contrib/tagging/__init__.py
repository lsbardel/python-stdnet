from .models import TaggedItem, Tag

def get_or_create(tag, lower_case = True):
    # Internal for adding or creating tags
    if isinstance(tag,Tag):
        return tag
    else:
        if lower_case:
            tag = tag.lower()
        try:
            return Tag.objects.get(name = tag)
        except Tag.DoesNotExist:
            return Tag(name = tag).save()


def addtag(obj, tag, lower_case = True):
    '''A a tag to an object.
If the object already has the tag associated with it do nothing.

:parameter obj: instance of :class:`stdnet.orm.StdModel`.
:parameter tag: a string for the tag name or a :class:`Tag` instance.
:parameter lower_case: if ``True`` tags are always lower case. Default ``True``.

It returns an instance of :class:`TaggedItem`.
'''
    model = obj._meta.model
    ctag = get_or_create(tag)
    tags = TaggedItem.objects.filter(object_id = obj.id,
                                     model_type = model,
                                     tag = ctag)
    if not tags.count():
        return TaggedItem(object_id = obj.id,
                          model_type = model,
                          tag = ctag).save()
    else:
        return tags[0]
    

def formodels(*models):
    '''Return a dictionary where keys are tag names and values are integers
    representing how many times the corresponding tag has been used against
    the Model classes in question.'''
    tags = {}
    for t in TaggedItem.objects.filter(model_type__in = models):
        tag = t.tag.name
        if tag in tags:
            tags[tag] += 1
        else:
            tags[tag] = 1
    return tags