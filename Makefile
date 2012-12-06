# simple makefile for development.

SRC=_vcfdbmodule.c
ext2: ${SRC}
	rm -f _vcfdb.so
	python setup.py build_ext --inplace

ext3: ${SRC}
	rm -f _vcfdb.so
	python3 setup.py build_ext --inplace

figs:
	cd docs/asy && make 

docs: ext2 figs 
	cd docs && make clean && make html

	
