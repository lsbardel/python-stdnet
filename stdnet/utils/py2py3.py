"""\
Simple python script which helps writing python 2.6 \
forward compatible code with python 3"""
import os
import sys
import types

ispy3k = int(sys.version[0]) >= 3


# Python 3
if ispy3k:
    string_type = str
    itervalues = lambda d: d.values()
    iteritems = lambda d: d.items()
    int_type = int
    zip = zip
    map = map
    long = int
    range = range

    from io import BytesIO, StringIO
    from itertools import zip_longest
    from urllib import parse as urlparse

    urlencode = urlparse.urlencode

    class UnicodeMixin(object):
        def __unicode__(self):
            return "{0} object".format(self.__class__.__name__)

        def __str__(self):
            return self.__unicode__()

        def __repr__(self):
            return "%s: %s" % (self.__class__.__name__, self)

    def native_str(s, encoding="utf-8"):
        if isinstance(s, bytes):
            return s.decode(encoding)
        return s


# Python 2
else:  # pragma: no cover
    string_type = unicode
    itervalues = lambda d: d.itervalues()
    iteritems = lambda d: d.iteritems()
    int_type = (types.IntType, types.LongType)
    from itertools import imap as map
    from itertools import izip as zip
    from itertools import izip_longest as zip_longest

    range = xrange
    long = long

    from urllib import urlencode

    import urlparse
    from cStringIO import StringIO

    BytesIO = StringIO

    class UnicodeMixin(object):
        def __unicode__(self):
            return unicode("{0} object".format(self.__class__.__name__))

        def __str__(self):
            return self.__unicode__().encode("utf-8", "ignore")

        def __repr__(self):
            return "%s: %s" % (self.__class__.__name__, self)

    def native_str(s, encoding="utf-8"):
        if isinstance(s, unicode):
            return s.encode(encoding)
        return s


is_string = lambda x: isinstance(x, string_type)
is_int = lambda x: isinstance(x, int_type)


def to_bytes(s, encoding=None, errors="strict"):
    """Returns a bytestring version of 's',
    encoded as specified in 'encoding'."""
    encoding = encoding or "utf-8"
    if isinstance(s, bytes):
        if encoding != "utf-8":
            return s.decode("utf-8", errors).encode(encoding, errors)
        else:
            return s
    if not is_string(s):
        s = string_type(s)
    return s.encode(encoding, errors)


def to_string(s, encoding=None, errors="strict"):
    """Inverse of to_bytes"""
    encoding = encoding or "utf-8"
    if isinstance(s, bytes):
        return s.decode(encoding, errors)
    if not is_string(s):
        s = string_type(s)
    return s
