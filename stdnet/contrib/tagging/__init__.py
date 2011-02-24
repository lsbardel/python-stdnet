from .models import TaggedItem

def cleantag(tag):
    return tag


def addtag(obj, tag):
    model = obj._meta.model
    ctag = cleantag(tag)
    tags = TaggedItem.objects.filter(object_id = obj.id,
                                     model_type = model,
                                     tag = tag)
    if not tags.count():
        return TaggedItem(object_id = obj.id,
                          model_type = model,
                          tag = tag).save()
    else:
        return tags[0]
    
