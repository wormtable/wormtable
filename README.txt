===============================================
Wormtable
===============================================

Wormtable is a write-once read-many table for large scale datasets.
It provides Python programmers with a simple and efficient method of 
storing, processing and searching datasets of essentially unlimited
size. A wormtable consists of a set of rows, each of which contains 
values belonging to a fixed number of columns. Rows are encoded 
in a custom binary format, designed to be flexible, compact and 
portable. These rows are then stored on disc using Berkeley DB,
a highly respected embedded database toolkit. Wormtable also 
supports efficient searching and retrieval of rows with particular
values through the use of indexes.

The Variant Call Format (VCF) is supported directly by wormtable
through a command line conversion program, vcf2wt. There is also a
command line utility wtadmin to manage wormtables, including the ability to 
dump values and add, remove and view indexes.

-------------
Documentation
-------------

Here's a quick example for the impatient::

        import wormtable as wt 
        with wt.open_table("example.wt") as t:
	    c = t.cursor("POS", "ALT", "REF")
	    for pos, alt, ref in c:
	    	print(pos, ":", alt, ":", ref)

Full documentation for ``wormtable`` is available at `<http://jeromekelleher.github.com/wormtable>`_.

------------
Installation
------------

Wormtable requires Berkeley DB, which is available for all major platforms.  
Any recent version of Berkeley DB should work, but the various versions 
have not been tested extensively. Development and testing have been 
carried out primarily on the DB 4.x series.

Once DB has been installed (see below) we can build the ``wormtable`` module using the 
standard Python `methods <http://docs.python.org/install/index.html>`_. For 
example, using pip we have ::
        
        $ sudo pip install wormtable

Or, we can manually download the package, unpack it and then run::
        
        $ python setup.py build
        $ sudo python setup.py install

Most of the time this will compile and install the module without difficulty.

It is also possible to download the latest development version of 
``wormtable`` from `github <https://github.com/jeromekelleher/wormtable>`_. 

----------------------
Installing Berkeley DB
----------------------

*****
Linux
*****
Wormtable is primarily developed on Linux and should work very well on any 
modern Linux distribution. Installing Berkeley DB is very easy on Linux 
distributions. 

On Debian/Ubuntu use::

        $ sudo apt-get install libdb-dev 

and on Red Hat/Fedora use::

        # yum install db4-devel

Other distributions and package managers should provide a similarly easy
option to install the DB development files.

********
Mac OS X
********

TODO: document installation on a mac. MacPorts/Homebrew/installer package?

*****
Unix
*****

Most Unix systems provide Berkeley DB packages. For example, on FreeBSD
we have::

    # pkg_add -r db48

If necessary, Berkeley DB can be built from source and installed manually quite 
easily.

******************
Potential problems
******************

On platforms that Berkeley DB is not available as part of the native packaging 
system (or DB was installed locally because of non-root access)
there can be issues with finding the correct headers and libraries
when compiling ``wormtable``. For example, on FreeBSD we get something 
like this::

        $ python setup.py build
        ... [Messages cut for brevity] ...
        _wormtablemodule.c:3727: error: 'DB_NEXT_NODUP' undeclared (first use in this function)
        _wormtablemodule.c:3733: error: 'DB_NOTFOUND' undeclared (first use in this function)
        _wormtablemodule.c:3739: error: 'DistinctValueIterator' has no member named 'cursor'
        _wormtablemodule.c:3739: error: 'DistinctValueIterator' has no member named 'cursor'
        _wormtablemodule.c:3740: error: 'DistinctValueIterator' has no member named 'cursor'
        error: command 'cc' failed with exit status 1

To remedy this we must set the 
``LDFLAGS`` and ``CFLAGS`` environment variables to 
their correct values. Unfortunately there is no simple method to do this 
and some knowledge of where your system keeps headers and libraries 
is needed.  For example on FreeBSD (after installing the ``db48`` package) we 
might use::
        
         $ CFLAGS=-I/usr/local/include/db48 LDFLAGS=-L/usr/local/lib/db48 python setup.py build

Some systems may also have very old Berkeley DB headers, which are not compatible 
with the modern API. For example, on NetBSD we get very simular errors to those seen 
above, even when we have set the paths to point to the correct locations.


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
Debian squeeze          ppc64           3.1.3           clang 1.1 
Debian wheezy           armv6l          2.7.3           gcc 4.6.3
Fedora 17               i386            2.7.3           gcc 4.7.2
Fedora 17               i386            3.2.3           gcc 4.7.2
FreeBSD 9.0             i386            3.2.2           gcc 4.2.2        
FreeBSD 9.0             i386            2.7.2           gcc 4.2.2        
FreeBSD 9.0             i386            3.1.4           clang 3.0 
================        ========        ======          ===========     

