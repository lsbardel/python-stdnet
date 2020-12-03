"""Object data mapper and advanced query manager for non relational
databases.
"""
from .backends import *
from .utils.exceptions import *
from .utils.version import get_version, stdnet_version

VERSION = stdnet_version(0, 9, 0, "alpha", 3)


__version__ = version = get_version(VERSION)
__license__ = "BSD"
__author__ = "Luca Sbardella"
__contact__ = "luca.sbardella@gmail.com"
__homepage__ = "https://github.com/lsbardel/python-stdnet"
CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Environment :: Plugins",
    "Environment :: Console",
    "Environment :: Web Environment",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: BSD License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 2",
    "Programming Language :: Python :: 2.6",
    "Programming Language :: Python :: 2.7",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.2",
    "Programming Language :: Python :: 3.3",
    "Programming Language :: Python :: Implementation :: PyPy",
    "Topic :: Utilities",
    "Topic :: Database",
    "Topic :: Internet",
]
