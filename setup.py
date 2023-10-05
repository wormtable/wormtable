import re
import sys
from distutils.core import setup, Extension


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



f = open("README.txt")
wormtable_readme = f.read()
f.close()
wormtable_version = parse_version("wormtable/__init__.py")

_wormtable_module = Extension('_wormtable',
    sources = ["_wormtablemodule.c", "halffloat.c"],
    libraries = ["db"])

setup(
    name = "wormtable",
    version = wormtable_version,
    description = "Write-once read-many data sets using Berkeley DB.",
    author = "Jerome Kelleher, Dan Halligan, Rob Ness",
    author_email = "jerome.kelleher@ed.ac.uk",
    url = "http://pypi.python.org/pypi/wormtable",
    keywords = ["Berkeley DB", "VCF", "Variant Call Format", "Bioinformatics"],
    license = "GNU LGPLv3+",
    platforms = ["POSIX"],
    classifiers = [
        "Programming Language :: C",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Development Status :: 4 - Beta",
        "Environment :: Other Environment",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)",
        "Operating System :: POSIX",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
    ],
    long_description = wormtable_readme,
    ext_modules = [_wormtable_module],
    packages = ['wormtable'],
    scripts = ["scripts/vcf2wt", "scripts/wtadmin", "scripts/gtf2wt"]
)


