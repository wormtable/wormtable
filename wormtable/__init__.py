"""
Simple module presenting the externally visible namespace.
"""

__version__ = '0.0.1-dev'

from .core import Schema 
from .core import Index 
from .core import Table 
from .core import TableBuilder 
from .vcf import vcf_schema_factory 
from .vcf import VCFTableBuilder
