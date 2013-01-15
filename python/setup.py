import re
from distutils.core import setup, Extension

f = open("README.txt")
wormtable_readme = f.read()
f.close()

# Following the recommendations of PEP 396 we parse the version number 
# out of the module.
def parse_version(module_file):
    """
    Parses the version string from the specified file.
    
    This implementation is ugly, but there doesn't seem to be a good way
    to do this in general at the moment.
    """ 
    f = open(module_file)
    s = f.read()
    f.close()
    match = re.findall("__version__ = '([^']+)'", s)
    return match[0]

wormtable_version = parse_version("wormtable.py") 

_wormtable_module = Extension('_wormtable', 
    sources = ["_wormtablemodule.c"],
    libraries = ["db"])

setup(
    name = "wormtable",
    version = wormtable_version, 
    description = "Write-once-read-many table for large datasets.",
    author = "Jerome Kelleher",
    author_email = "jerome.kelleher@ed.ac.uk",
    url = "http://pypi.python.org/pypi/wormtable", 
    keywords = ["add", "some", "keywords"], 
    license = "GNU GPLv3",
    platforms = ["POSIX"], 
    classifiers = [
        "Programming Language :: C",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Environment :: Other Environment",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: POSIX",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
    ],
    long_description = wormtable_readme,
    ext_modules = [_wormtable_module],
    py_modules = ['wormtable']
)

    
