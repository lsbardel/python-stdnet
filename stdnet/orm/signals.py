from stdnet.dispatch import Signal

__all__ = ['class_prepared',
           'pre_init',
           'post_init',
           'pre_save',
           'post_save',
           'pre_delete',
           'post_delete']


class_prepared = Signal(providing_args=["class"])

pre_init = Signal(providing_args=["instance", "args", "kwargs"])
post_init = Signal(providing_args=["instance"])

pre_save = Signal(providing_args=["instance", "raw"])
post_save = Signal(providing_args=["instance", "raw", "created"])

pre_delete = Signal(providing_args=["instance"])
post_delete = Signal(providing_args=["instance"])