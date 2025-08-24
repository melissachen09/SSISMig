"""SSIS to Airflow/dbt Migration Tool.

A comprehensive tool for migrating SSIS packages to modern data engineering platforms.
"""

__version__ = "0.1.0"
__author__ = "AI Assistant"
__description__ = "SSIS to Airflow/dbt Migration Tool"

from . import parser
from . import generators  
from . import models
from . import adapters
from . import cli

__all__ = ["parser", "generators", "models", "adapters", "cli"]