# Taken from Python 2.7
import sys
import os
import glob


def _resolve_name(name, package, level):
    """Return the absolute name of the module to be imported."""
    if not hasattr(package, 'rindex'):
        raise ValueError("'package' not set to a string")
    dot = len(package)
    for x in xrange(level, 1, -1):
        try:
            dot = package.rindex('.', 0, dot)
        except ValueError:
            raise ValueError("attempted relative import beyond top-level "
                              "package")
    return "%s.%s" % (package[:dot], name)

def import_module(name, package=None):
    """Import a module.

    The 'package' argument is required when performing a relative import. It
    specifies the package to use as the anchor point from which to resolve the
    relative import to an absolute import.

    """
    if name.startswith('.'):
        if not package:
            raise TypeError("relative imports require the 'package' argument")
        level = 0
        for character in name:
            if character != '.':
                break
            level += 1
        name = _resolve_name(name[level:], package, level)
    __import__(name)
    return sys.modules[name]


def expand_star(mod_name):
    """Expand something like 'unuk.tasks.*' into a list of all the modules
    there.
    """
    expanded = []
    mod_dir  = os.path.dirname(__import__(mod_name[:-2], {}, {}, ['']).__file__)
    for f in glob.glob1(mod_dir, "[!_]*.py"):
        expanded.append('%s.%s' % (mod_name[:-2], f[:-3]))
    return expanded


def import_modules(modules):
    '''Safely import a list of *modules*
    '''
    mods = []
    for mname in modules:
        if mname.endswith('.*'):
            to_load = expand_star(mname)
        else:
            to_load = [mname]
        for module in to_load:
            try:
                mods.append(import_module(module))
            except ImportError, e:
                pass
    return mods