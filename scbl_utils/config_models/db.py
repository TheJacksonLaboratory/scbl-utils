from pathlib import Path
from typing import Literal

from ..pydantic_model_config import StrictBaseModel


class DBConfig(StrictBaseModel, frozen=True):
    database: str
    drivername: Literal['sqlite'] = 'sqlite'
