paper.pdf: paper.tex paper.bib
	pdflatex paper.tex
	bibtex paper
	pdflatex paper.tex
paper.ps: paper.dvi 
	dvips paper

paper.dvi: paper.tex paper.bib 
	latex paper.tex
	bibtex paper
	latex paper.tex
	latex paper.tex


clean:
	rm -f *.log *.dvi *.aux
	rm -f *.blg *.bbl
	rm -f *.eps *.[1-9]	
	rm -f src/*.mpx *.mpx
	
mrproper:
	rm -f *.ps *.pdf
