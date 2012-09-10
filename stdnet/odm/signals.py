from stdnet.utils.dispatch import Signal

__all__ = ['class_prepared',
           'pre_init',
           'post_init',
           'pre_commit',
           'pre_delete',
           'post_commit',
           'post_delete']


class_prepared = Signal(providing_args=["class"])

pre_init = Signal(providing_args=["instance", "args", "kwargs"])
post_init = Signal(providing_args=["instance"])

pre_commit = Signal(providing_args=["instances", "transaction"])
pre_delete = Signal(providing_args=["instances", "transaction"])
post_commit = Signal(providing_args=["instances", "session", "transaction"])
post_delete = Signal(providing_args=["instances", "session", "transaction"])