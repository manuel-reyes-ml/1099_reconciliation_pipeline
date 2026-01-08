#Turn this directory into a package by adding an __init__.py file
#Dosctring for the package
"""
1099 Reconciliation Pipeline

This package contains the core modules for:

- Loading Relius and Matrix Excel exports
- Cleaning and normalizing data
- Matching transactions
- Building 1099 correction files

Subpackages:
- core
- cleaning
- engines
- visualization
- outputs

"""

#Import modules to be exposed at the package level
from . import core, cleaning, engines, visualization, outputs
__all__ = [
    "core",
    "cleaning",
    "engines",
    "visualization",
    "outputs",
]
