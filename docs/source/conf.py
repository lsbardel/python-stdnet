# -*- coding: utf-8 -*-
#
import sys
import os
from datetime import date
os.environ['BUILDING-PULSAR-DOCS'] = 'yes'
# If your extensions are in another directory, add it here.
source_dir = os.path.split(os.path.abspath(__file__))[0]
docs_dir   = os.path.split(source_dir)[0]
base_dir   = os.path.split(docs_dir)[0]
sys.path.append(os.path.join(source_dir, "_ext"))
sys.path.append(base_dir)
import stdnet
version = stdnet.__version__
release = version
year = date.today().year
import runtests # so that it import pulsar if available
# -- General configuration -----------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = ['sphinx.ext.autodoc', 'sphinx.ext.todo', 'sphinx.ext.pngmath']

# Beta version is published in github pages
if stdnet.VERSION[3] == 'beta':
    extensions.append('sphinxtogithub')
html_context = {'release_version': stdnet.VERSION[3] == 'final'}

# The suffix of source filenames.
source_suffix = '.rst'

# The encoding of source files.
#source_encoding = 'utf-8'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = 'python-stdnet'
copyright = '2009-%s, %s' % (year, stdnet.__author__)

html_theme = 'pulsar'

templates_path = ['_templates']
html_static_path = ['_static']
html_theme_path = ["_theme"]
html_sidebars = {
    'index': ['sidebarlogo.html', 'sidebarintro.html',
              'sourcelink.html', 'searchbox.html'],
    '**': ['sidebarlogo.html', 'localtoc.html', 'relations.html',
           'sourcelink.html', 'searchbox.html'],
}
exclude_trees = []
pygments_style = 'sphinx'

# Output file base name for HTML help builder.
htmlhelp_basename = 'PythonStdNetdoc'

# -- Options for LaTeX output --------------------------------------------------

# The paper size ('letter' or 'a4').
#latex_paper_size = 'letter'

# The font size ('10pt', '11pt' or '12pt').
#latex_font_size = '10pt'

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, documentclass [howto/manual]).
latex_documents = [
  ('index', 'PythonStdNet.tex', 'Python Stdnet Documentation',
   'Luca Sbardella', 'manual'),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
#latex_logo = None

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
#latex_use_parts = False

# Additional stuff for the LaTeX preamble.
#latex_preamble = ''

# Documents to append as an appendix to all manuals.
#latex_appendices = []

# If false, no module index is generated.
#latex_use_modindex = True
