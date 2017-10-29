from __future__ import absolute_import, division, print_function
import warnings

from .utils import *
from .core import *
from .env import CondaCreator, zip_path
from .yarn_api import YARNAPI


try:
    from dask_yarn import DaskYARNCluster as _DaskYARNCluster

    class DaskYARNCluster(_DaskYARNCluster):
        def __init__(self, *args, **kwargs):
            warnings.warn("DeprecationWarning: `knit.DaskYARNCluster` is "
                          "deprecated, please use "
                          "`dask_yarn.DaskYARNCluster` instead")
            super().__init__(*args, **kwargs)
except ImportError:
    pass

__version__ = "0.2.3"
