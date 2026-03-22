from __future__ import annotations

from dotenv import load_dotenv
from .config_manager import load_config as load_hierarchical_config, ConfigSchema

load_dotenv()


def load_config() -> ConfigSchema:
    """
    Load configuration using the hierarchical configuration manager
    This maintains backward compatibility while implementing the new architecture
    """
    return load_hierarchical_config()


# For backward compatibility, expose the Config class as well
Config = ConfigSchema
