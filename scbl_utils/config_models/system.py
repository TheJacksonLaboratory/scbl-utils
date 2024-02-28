from pathlib import Path

from pydantic import DirectoryPath

from ..pydantic_model_config import StrictBaseModel


class SystemConfig(StrictBaseModel, frozen=True):
    delivery_parent_dir: DirectoryPath = Path('/sc/service/delivery/')
