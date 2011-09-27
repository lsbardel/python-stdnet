#
# Required by Cython to build Hiredis extensions
#
# Requires numpy
#
import os
from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

try:
    import numpy
    include_dirs = [numpy.get_include()]
except Importerror:
    include_dirs = []

ext_modules  = Extension('stdnet.lib.hr', ['lib/src/hr.pyx',
                                           'lib/hiredismin/hiredis.c',
                                           'lib/hiredismin/sds.c',
                                           'lib/src/reader.c'])

base_path = os.path.split(os.path.abspath(__file__))[0]
include_dirs.append(os.path.join(base_path,'hiredismin'))
include_dirs.append(ext_include = os.path.join(base_path,'src'))


libparams = {
             'ext_modules': [ext_modules],
             'cmdclass': {'build_ext' : build_ext},
             'include_dirs': include_dirs
             }
