"""
Module for determining information about available compute capabilities.
"""

import multiprocessing


def get_existing_cpu_count() -> int:
    """
    Returns existing CPU count.
    """
    return multiprocessing.cpu_count()
