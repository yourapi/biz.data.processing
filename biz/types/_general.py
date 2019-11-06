import biz.pandas as bp
from biz import pandas as pd
import re, os

###############################################################################
# This module contains functions for validation, transformation of standard types
# Every (mini-)module for a type contains functions for validation and transformation.
# When a function is not present, no transformation is applied.
# The functions in this module serve as example for all possible converters, none
# of them will actually be executed.
# If 'pattern'  is specified and 'validate' is not specified, a function is created which 
# validates a value with the specified pattern.
###############################################################################

type_input = str  # Can be list of types. Any of the specified types is allowed as input
type_output = str  # Can be list of types  ToDo: put types in transformation-functions!! At thyis moment, types have no added value.
pattern = '.*'
# When a pattern is specified, it is used to construct a validate-function if it doesn't exist.

def validate(value):
    """Specification of a validate-function overrules an automatic validate-function.
    Always put the date of modification in the comment so the function is always recognized as changed in the framework."""
    return True

def normalize(value):
    """The normalize-function returns a cleaned up version of the value. Error-handling is added by the framework, 
    so no type-checking or error-handling necessary. When an error is encountered, the input-value is returned unchanged."""
    return type_output(value)

def cat25(value):
    """Optional function for specific patterns for this type, like the @-sign in an e-mail.
    When the value falls into this category, return a prototypical value,like a@b.cc for an e-mail.
    When it doesn't fall into this category, return a string-value 'no_cat', so the categorization can be executed correctly.
    no_cat should be: no_ip, no_email, no_postcode etc
    The function is executed in the order specified by its index. The standard categorizers are numbered cat10...cat99
    so the categorizers can be put anywhere in between (like the methodology for debugging levels)"""
    return 'no_cat'

def source_to_dataframe(value):
    "Transforms the value before storing it in the data-store (most likely a dataframe-column)"
    return value

def dataframe_to_hdf(value):
    "store the value from a column in a dataframe to an HDF-store."
    return value

def hdf_to_dataframe(value):
    "Transform a value from an HDF-store into a dataframe. Generally, there is no transformation."
    return value

def dataframe_to_human(value):
    "Make a value suitable for human presentation. The internal representation of a value can be different from normal human consumption."
    return value

if __name__ == '__main__':
    test()