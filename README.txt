===============================================
Wormtable
===============================================

Wormtable is a write-once read-many table for large scale datasets.
It provides Python programmers with a simple and efficient method of 
storing, processing and searching datasets of essentially unlimited
size. A wormtable consists of a set of rows, each of which contains 
values belonging to a fixed number of columns. Rows are encoded 
in a custom binary format, designed to be flexible, compact and 
portable. Rows are stored in a data file, and the offsets and lengths
of these rows are stored in a Berkeley DB database 
to support efficient random access.  Wormtable also 
supports efficient searching and retrieval of rows with particular
values through the use of indexes, also based on Berkeley DB.

The Variant Call Format (VCF) is supported directly by wormtable
through a command line conversion program, vcf2wt. There is also a
command line utility wtadmin to manage wormtables, including the ability to 
dump values and add, remove and view indexes.

-------------
Documentation
-------------

Full documentation for ``wormtable`` is available at 
`<http://pythonhosted.org/wormtable>`_.

------------
Installation
------------

*******************************
Quick install for Debian/Ubuntu
*******************************

If you are running Debian or Ubuntu, this should get you up and running quickly::

        $ sudo apt-get install python-dev libdb-dev
        $ sudo pip install wormtable

For Python 3, use ``python3-dev`` and ``pip3``.

********************
General instructions
********************

Once Berkeley DB has been installed (see below) we can build the ``wormtable`` module using the 
standard Python `methods <http://docs.python.org/install/index.html>`_. For 
example, using pip we have ::
        
        $ sudo pip install wormtable

Or, we can manually download the package, unpack it and then run::
        
        $ python setup.py build
        $ sudo python setup.py install

Most of the time this will compile and install the module without difficulty.

It is also possible to download the latest development version of 
``wormtable`` from `github <https://github.com/wormtable/wormtable>`_. 


**************
Python 2.6/3.1
**************

Wormtable requires the ``argparse`` package, which was introduced to the 
standard library for version 3.2 (it is also included in 2.7). For users 
of older Python versions, the ``argparse`` module must be installed for 
the command line utilities to work::

        $ sudo pip install argparse 

This is not necessary for recent versions of Python.

----------------------
Installing Berkeley DB
----------------------

Wormtable requires Berkeley DB (version 4.8 or later),
which is available for all major platforms.  

*****
Linux
*****

Installing Berkeley DB is very easy on Linux distributions. 

On Debian/Ubuntu use::

        $ sudo apt-get install libdb-dev 

and on Red Hat/Fedora use::

        # yum install libdb-devel 

Other distributions and package managers should provide a similarly easy
option to install the DB development files.

********
Mac OS X
********

Berkeley DB can be installed from source on a mac, via 
`macports <https://www.macports.org/>`_ or 
`homebrew <http://mxcl.github.io/homebrew/>`_.

For MacPorts, to install e.g. v5.3 ::

    $ sudo port install db53
    
Then, to build/install wormtable, we need to set the CFLAGS and LDFLAGS environment 
variables to use the headers and libraries in /opt::
 
    $ CFLAGS=-I/opt/local/include/db53 LDFLAGS=-L/opt/local/lib/db53/ python setup.py build
    $ sudo python setup.py install    
    
For Homebrew, get the current Berkeley DB version and again build wormtable
after setting CFLAGS and LDFLAGS appropriately::

    $ brew install berkeley-db
    $ CFLAGS=-I/usr/local/Cellar/berkeley-db/5.3.21/lib/ LDFLAGS=-I/usr/local/Cellar/berkeley-db/5.3.21/lib/ python setup.py build
    $ sudo python setup.py install

For more details of Berkely DB versions, see here: https://www.macports.org/ports.php?by=category&substr=databases


***************
Other Platforms
***************

On platforms that Berkeley DB is not available as part of the native packaging 
system (or DB was installed locally because of non-root access)
there can be issues with finding the correct headers and libraries
when compiling ``wormtable``. For example, 
if we add the DB 4.8 package on FreeBSD using:: 
        
        # pkg_add -r db48

we get the following errors when we try to install wormtable::

        $ python setup.py build
        ... [Messages cut for brevity] ...
        _wormtablemodule.c:3727: error: 'DB_NEXT_NODUP' undeclared (first use in this function)
        _wormtablemodule.c:3733: error: 'DB_NOTFOUND' undeclared (first use in this function)
        _wormtablemodule.c:3739: error: 'DistinctValueIterator' has no member named 'cursor'
        _wormtablemodule.c:3739: error: 'DistinctValueIterator' has no member named 'cursor'
        _wormtablemodule.c:3740: error: 'DistinctValueIterator' has no member named 'cursor'
        error: command 'cc' failed with exit status 1

This is because the compiler does not know where to find the headers and library 
files for Berkeley DB.  
To remedy this we must set the 
``LDFLAGS`` and ``CFLAGS`` environment variables to 
their correct values. Unfortunately there is no simple method to do this 
and some knowledge of where your system keeps headers and libraries 
is needed. To complete the installation for the FreeBSD example above, 
we can do the following::
        
         $ CFLAGS=-I/usr/local/include/db48 LDFLAGS=-L/usr/local/lib/db48 python setup.py build
         $ sudo python setup.py install


----------
Test suite
----------

Wormtable has an extensive suite of tests to ensure that data
is stored correctly.
It is a good idea to run these immediately after installation::

        $ python tests.py


****************
Tested platforms
****************

Wormtable is higly portable, and 
has been successfully built and tested 
on the following platforms:

================        ========        ======          ===========     
Operating system        Platform        Python          Compiler        
================        ========        ======          ===========     
Ubuntu 13.04            x86-64          2.7.4           gcc 4.7.3       
Ubuntu 13.04            x86-64          3.3.1           gcc 4.7.3       
Ubuntu 13.04            x86-64          2.7.4           clang 3.2.1     
Debian squeeze          x86-64          2.6.6           gcc 4.4.5       
Debian squeeze          x86-64          3.1.3           gcc 4.4.5        
Debian squeeze          x86-64          3.1.3           clang 1.1 
Debian squeeze          ppc64           2.6.6           gcc 4.4.5	    
Debian squeeze          ppc64           3.1.3           gcc 4.4.5	
Debian wheezy           armv6l          2.7.3           gcc 4.6.3
Fedora 17               i386            2.7.3           gcc 4.7.2
Fedora 17               i386            3.2.3           gcc 4.7.2
FreeBSD 9.0             i386            3.2.2           gcc 4.2.2        
FreeBSD 9.0             i386            2.7.2           gcc 4.2.2        
FreeBSD 9.0             i386            3.1.4           clang 3.0 
OS X 10.8.4             x86-64          2.7.2           clang 4.2
SunOS 5.10              SPARC           3.3.2           gcc 4.8.0 
================        ========        ======          ===========     

