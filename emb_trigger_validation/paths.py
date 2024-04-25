# coding: utf-8

"""
Convenient constant variables, which point to important paths of this project.
"""

import os


__all__ = [
    "BASE_DIR", "CONFIG_DIR", "DATA_DIR", "MODULES_DIR", "SOFTWARE_DIR", "CONFIG_FILE", "CONFIG_DATA_DIR", "JOBS_DIR", "LOCAL_STORE_DIR", "WLCG_STORE_DIR",
]



# root directory of the project
BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

# important sub-directories in BASE_DIR
CONFIG_DIR = os.path.join(BASE_DIR, "config")
DATA_DIR = os.path.join(BASE_DIR, "data")
MODULES_DIR = os.path.join(BASE_DIR, "modules")
SOFTWARE_DIR = os.path.join(DATA_DIR, "software")

# file containing the base configuration
CONFIG_FILE = os.path.join(CONFIG_DIR, "config.yaml")

# output directory for job submission files
JOBS_DIR = os.path.join(DATA_DIR, "jobs")

# output directories for output files of the config module and cache files
CONFIG_DATA_DIR = os.path.join(DATA_DIR, "config")
CACHE_DIR = os.path.join(DATA_DIR, "cache")

# output directory for task outputs (local and WLCG)
LOCAL_STORE_DIR = os.path.join(DATA_DIR, "store")
WLCG_STORE_DIR = os.path.join(os.path.basename(BASE_DIR), "data", "store")

# store for outputs of the config manager
CONFIG_STORE_DIR = os.path.join(DATA_DIR, "config")
