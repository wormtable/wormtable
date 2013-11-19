# simple makefile for development.

SRC=_wormtablemodule.c
CFLAGS=-Wpointer-arith

ext3: ${SRC}
	CFLAGS=${CFLAGS} python3 setup.py build_ext --inplace

ext2: ${SRC}
	python setup.py build_ext --inplace

ctags:
	ctags *.c *.py test/*.py

figs:
	cd docs/asy && make 

docs: ext2 figs 
	cd docs && make clean && make html

	
