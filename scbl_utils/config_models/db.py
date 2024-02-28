from typing import Literal

from pydantic import FilePath
from scbl_db import Base
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session, sessionmaker

from ..pydantic_model_config import StrictBaseModel


class DBConfig(StrictBaseModel, frozen=True):
    database: str
    drivername: Literal['sqlite'] = 'sqlite'
