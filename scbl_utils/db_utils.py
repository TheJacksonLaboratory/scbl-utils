from typing import Literal

from sqlalchemy import URL, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from .pydantic_model_config import StrictBaseModel


class DBConfig(StrictBaseModel, frozen=True):
    database: str  # TODO: add extra validation for this to make sure it's a valid database
    drivername: Literal['sqlite'] = 'sqlite'

    def sessionmaker(
        self, db_base_class: type[DeclarativeBase]
    ) -> sessionmaker[Session]:
        url = URL.create(database=self.database, drivername=self.drivername)
        engine = create_engine(url)
        Session = sessionmaker(engine)
        db_base_class.metadata.create_all(engine)

        return Session
