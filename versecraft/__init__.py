"""
VerseCraft: Poetic Product Creation

This module provides tools for creating and validating poems for technical products.
"""

from .originality_validator import (
    OriginalityValidator,
    validate_poem_originality
)

__all__ = [
    'OriginalityValidator',
    'validate_poem_originality'
]
