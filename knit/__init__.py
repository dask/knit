from __future__ import absolute_import, division, print_function
import warnings

from .utils import *
from .core import *
from .env import CondaCreator, zip_path
from .yarn_api import YARNAPI


class DaskYARNCluster(object):
    def __new__(cls, *args, **kwargs):
        import dask_yarn
        warnings.warn("Deprecated use of DaskYARNCluster in the knit "
                      "namespace. Use the dask_yarn package instead.")
        o = dask_yarn.DaskYARNCluster.__new__(dask_yarn.DaskYARNCluster)
        o.__init__(*args, **kwargs)
        return o

__version__ = "0.2.2"
