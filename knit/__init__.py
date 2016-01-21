from __future__ import absolute_import, division, print_function

from .utils import *
from .core import *

__version__ = '0.0.1'

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
