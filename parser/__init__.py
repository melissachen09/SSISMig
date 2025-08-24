"""SSIS DTSX parser package.

This package contains all the components for parsing SSIS .dtsx files
and converting them to the Intermediate Representation (IR) format.
"""

from .to_ir import DTSXToIRConverter
from .dtsx_reader import DTSXReader, DTSXParseError
from .dataflow_parser import DataFlowParser
from .precedence_parser import PrecedenceParser
from .expressions import SSISExpressionParser
from .secure import SSISSecurityHandler

__all__ = [
    "DTSXToIRConverter",
    "DTSXReader", 
    "DTSXParseError",
    "DataFlowParser",
    "PrecedenceParser", 
    "SSISExpressionParser",
    "SSISSecurityHandler"
]