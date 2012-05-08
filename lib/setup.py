#
# Required by Cython to build Hiredis extensions
#
import os
import sys
from distutils.core import setup
from distutils.extension import Extension
from distutils.errors import (CCompilerError, DistutilsExecError,
                              DistutilsPlatformError)
try:
    from Cython.Distutils import build_ext
    cython_message = None
except ImportError:
    from distutils.command.build_ext import build_ext
    cython_message = 'Cannot build C extensions, Cython is not installed.'
    
try:
    import numpy
    include_dirs = [numpy.get_include()]
except ImportError:
    include_dirs = []
    
ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError) 
if sys.platform == 'win32' and sys.version_info > (2, 6):
   # 2.6's distutils.msvc9compiler can raise an IOError when failing to
   # find the compiler
   ext_errors += (IOError,)

class BuildFailed(Exception):

    def __init__(self, msg=None):
        if not msg:
            msg = str(sys.exc_info()[1])
        self.msg = msg


class tolerant_build_ext(build_ext):
    # This class allows C extension building to fail. From SQLAlchemy

    def run(self):
        if cython_message:
            raise BuildFailed(cython_message) 
        try:
            build_ext.run(self)
        except DistutilsPlatformError:
            raise BuildFailed()

    def build_extension(self, ext):
        if cython_message:
            raise BuildFailed(cython_message)
        try:
            build_ext.build_extension(self, ext)
        except ext_errors:
            raise BuildFailed()
        except ValueError:
            # this can happen on Windows 64 bit, see Python issue 7511
            if "'path'" in str(sys.exc_info()[1]): # works with both py 2/3
                raise BuildFailed()
            raise

################################################################################
##    EXTENSIONS

ext_modules  = Extension('stdnet.lib.hr', ['lib/src/hr.pyx',
                                           'lib/hiredis/hiredis.c',
                                           'lib/hiredis/sds.c',
                                           'lib/src/reader.c'])

base_path = os.path.split(os.path.abspath(__file__))[0]
include_dirs.append(os.path.join(base_path,'hiredis'))
include_dirs.append(os.path.join(base_path,'src'))

libparams = {
             'ext_modules': [ext_modules],
             'cmdclass': {'build_ext' : tolerant_build_ext},
             'include_dirs': include_dirs
             }
