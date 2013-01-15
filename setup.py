import re
from distutils.core import setup, Extension

f = open("README.txt")
vcfdb_readme = f.read()
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

vcfdb_version = parse_version("vcfdb/__init__.py") 

_vcfdb_module = Extension('_vcfdb', 
    sources = ["_vcfdbmodule.c"],
    libraries = ["db"])

setup(
    name = "vcfdb",
    version = vcfdb_version, 
    description = "Store and search VCF data with Berkeley DB",
    author = "Jerome Kelleher",
    author_email = "jerome.kelleher@ed.ac.uk",
    url = "http://pypi.python.org/pypi/vcfdb", 
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
    long_description = vcfdb_readme,
    ext_modules = [_vcfdb_module],
    packages = ['vcfdb']
)

    
