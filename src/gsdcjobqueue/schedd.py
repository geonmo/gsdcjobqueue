import concurrent.futures
import functools
import logging
import os
import re
import htcondor
logger = logging.getLogger(__name__)


def acquire_schedd():
    """Acquire a htcondor.Schedd object

    """
    return htcondor.Schedd()


# Pick a schedd once on import
# Would prefer one per cluster but there is a quite scary weakref.finalize
# that depends on it. Wrap with a memoized getter to avoid setup if not used
SCHEDD = functools.lru_cache(acquire_schedd)
# The htcondor binding has a global lock so there's no point in using more than
# one pool for asyncio run_in_executor
SCHEDD_POOL = concurrent.futures.ThreadPoolExecutor(1)
