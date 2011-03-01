#
# Required by Cython to build Hiredis extensions
#
# Requires numpy
#
import os
from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext
import numpy

ext_modules  = Extension('stdnet.lib.hr', ['vendor/src/hr.pyx',
                                           'vendor/hiredis/hiredis.c',
                                           'vendor/hiredis/net.c',
                                           'vendor/hiredis/sds.c',
                                           'vendor/hiredis/async.c'])

base_path = os.path.split(os.path.abspath(__file__))[0]
hiredis_include = os.path.join(base_path,'hiredis')
ext_include = os.path.join(base_path,'hiredis')


libparams = {
             'ext_modules': [ext_modules],
             'cmdclass': {'build_ext' : build_ext},
             'include_dirs': [numpy.get_include(),
                              ext_include,
                              hiredis_include]
             }
