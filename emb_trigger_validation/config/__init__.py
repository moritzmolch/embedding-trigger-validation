# coding: utf-8

"""
Configuration of production campaigns. This includes the used datasets as well as the trigger paths under investigation.
"""

from emb_trigger_validation.paths import CONFIG_FILE

from .config_manager import ConfigManager


__all__ = [
    "campaigns_config",
]


config_manager = ConfigManager(CONFIG_FILE)
