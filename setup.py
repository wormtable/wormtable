from distutils.core import setup, Extension

homepage = "http://www.homepages.ed.ac.uk/jkellehe/"
f = open("README.rst")
ercs_readme = f.read()
f.close()

d = "lib/"
_ercs_module = Extension('_ercs', 
    sources = ["_ercsmodule.c", d + "util.c", d + "kdtree.c", d + "ercs.c", 
            d + "torus.c"],
    include_dirs = [d],
    libraries = ["gsl", "gslcblas"])

setup(
    name = "ercs",
    version = "1.0.0",
    description = "Extinction/recolonisation coalescent simulator",
    author = "Jerome Kelleher",
    author_email = "jerome.kelleher@ed.ac.uk",
    url = homepage + "ercs",
    download_url = homepage + "download/ercs-1.0.tar.gz",
    keywords = ["simulation", "coalescent"],
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
    long_description = ercs_readme,
    ext_modules = [_ercs_module],
    py_modules = ['ercs']
)

    
