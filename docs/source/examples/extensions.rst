.. _c-extensions:

=======================
C/C++ Extensions
=======================

If cython_ is available during installation, stdnet will compile and install
a set of extensions which greatly speed-up the library when large amount of data
is retrieved or saved.


Installing Cython
======================

In linux or Mac OS it is as simple as::

    pip install cython
    
for windows, you are better off to download and install a binary distribution
from `this site`_.


.. _cython: http://cython.org/
.. _`this site`: http://www.lfd.uci.edu/~gohlke/pythonlibs/#cython