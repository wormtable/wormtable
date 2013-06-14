
all: ../images/row-format.png 

../images/row-format.png: row-format.asy
	asy row-format.asy -o tmp.eps
	convert -density 300 -units PixelsPerInch tmp.eps ../images/row-format.png
	rm -f tmp.eps

