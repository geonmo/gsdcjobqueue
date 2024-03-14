"""A dask-jobqueue plugin for the GSDC Condor queue
"""
from .cluster import GSDCCondorCluster, GSDCCondorJob
from .version import version as __version__

__all__ = ["__version__", "GSDCCondorJob", "GSDCCondorCluster"]
