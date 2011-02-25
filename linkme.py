'''Python script for linkin the ``stdnet`` directory to your python distribution package directory.
Linux only.
'''
import sys
import os
from distutils.sysconfig import get_python_lib

module = 'stdnet'


parentdir = lambda dir,up=1: dir if not up else parentdir(os.path.split(dir)[0],up-1)

def filedir(name):
    return parentdir(os.path.abspath(name))

if __name__ == '__main__':
    plib = os.path.join(get_python_lib(),module)
    fname = os.path.join(filedir(__file__),module)
    os.symlink(fname, plib)