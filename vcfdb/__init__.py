"""
Prototype implementation of the Berkeley DB VCF record store.
"""

__version__ = '0.0.1-dev'

from .core import Schema 
from .vcf import VCFSchemaFactory 
from .vcf import VCFDatabaseWriter
