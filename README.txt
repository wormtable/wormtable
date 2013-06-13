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

The Variant Call Format (VCF) format is supported directly by wormtable
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

Wormtable requires Berkeley DB. (TODO: document requirements and installation


*****
Tests
*****

Wormtable has an extensive suite of tests to ensure that data
is stored correctly.
It is a good idea to run these immediately after installation::

        $ python tests.py


****************
Tested platforms
****************

Wormtable has been successfully built and tested on the following platforms
(TODO: update):

================        ========        ======          ========
Operating system        Platform        Python          Compiler
================        ========        ======          ========
Ubuntu 8.04             i386            2.5.2           gcc 4.2.3 
NetBSD 5.0              i386            2.7.3           gcc 4.1.3
Fedora 17               i386            2.7.3           gcc 4.7.2
Fedora 17               i386            3.2.3           gcc 4.7.2
Cygwin                  i386            2.6.8           gcc 4.5.3
Ubuntu 12.04            x86-64          2.7.3           gcc 4.6.3
Ubuntu 12.04            x86-64          3.2.3           gcc 4.6.3
FreeBSD 9.0             i386            3.2.2           gcc 4.2.2        
FreeBSD 9.0             i386            2.7.2           gcc 4.2.2        
FreeBSD 9.0             i386            3.1.4           clang 3.0 
Solaris 11              x86-64          2.6.4           Sun C 5.12
Mac OSX 10.6.8          x86-64          2.6.1           gcc 4.2.1
Mac OSX 10.6.8          x86-64          3.2.3           gcc 4.2.1
Mac OS X 10.4.11        ppc             3.2.3           gcc 4.0.1
Mac OS X 10.4.11        ppc             2.7.3           gcc 4.0.1
Debian wheezy           armv6l          2.7.3           gcc 4.6.3
Debian squeeze          ppc64           2.6.6           gcc 4.4.5	
Debian squeeze          ppc64           3.1.3           gcc 4.4.5	
================        ========        ======          ========

