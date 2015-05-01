"""
This init module provides some simple exception classes
and an alias to the main KiveAPI object.
"""

class KiveAuthException(Exception):
    pass


class KiveServerException(Exception):
    pass


class KiveMalformedDataException(Exception):
    pass

# Forward the class declaration
from kiveapi import KiveAPI as kapi
KiveAPI = kapi
