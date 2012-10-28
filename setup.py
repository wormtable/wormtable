from distutils.core import setup

homepage = "http://www.homepages.ed.ac.uk/jkellehe/"
f = open("README.rst")
vcfdb_readme = f.read()
f.close()

setup(
    name = "vcfdb",
    version = "0.2.dev",
    description = "Store and search VCF data with Berkeley DB",
    author = "Jerome Kelleher",
    author_email = "jerome.kelleher@ed.ac.uk",
    url = homepage + "vcfdb",
    download_url = homepage + "download/vcfdb-1.0.tar.gz",
    # TODO Fix these keywords and classifiers
    keywords = ["Databases", "bioinformatics"],
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
    packages = ['vcfdb']
)

    
