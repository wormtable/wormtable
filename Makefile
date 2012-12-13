# simple makefile for development.

SRC=_vcfdbmodule.c

ext3: ${SRC}
	python3 setup.py build_ext --inplace

ext2: ${SRC}
	python setup.py build_ext --inplace


figs:
	cd docs/asy && make 

docs: ext2 figs 
	cd docs && make clean && make html

	
