from collections.abc import Mapping
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    DirectoryPath,
    Field,
    FilePath,
    HttpUrl,
    NonNegativeInt,
    StringConstraints,
)
from pydantic.dataclasses import dataclass
from scbl_db import ORDERED_MODELS, Base
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session, sessionmaker


@dataclass(frozen=True, kw_only=True)
class ColumnTargetMapping:
    column: str
    targets: set[
        Annotated[
            str,
            StringConstraints(
                pattern='|'.join(
                    rf'{model_name}\.[\w.]+' for model_name in ORDERED_MODELS
                )
            ),
        ]
    ]
    mapper: Mapping[str, Any]


@dataclass(frozen=True, kw_only=True)
class GWorksheetConfig:
    replace: Mapping[str, Any]
    head: NonNegativeInt = 0
    type_converters: Mapping[str, str]
    empty_means_drop: set[str]
    cols_to_targets: list[ColumnTargetMapping]


@dataclass(frozen=True, kw_only=True)
class GSPreadsheetConfig:
    spreadsheet_url: HttpUrl
    main_sheet_id: str
    worksheets: Mapping[
        Annotated[str, StringConstraints(pattern=r'^\d+$')], GWorksheetConfig
    ]


@dataclass(frozen=True, kw_only=True)
class SystemConfig:
    config_path: FilePath
    delivery_parent_dir: DirectoryPath = Field(
        default=Path('/sc/service/delivery/'), validate_default=True
    )


@dataclass(frozen=True, kw_only=True)
class DBConfig:
    database: str  # TODO: add extra validation for this to make sure it's a valid database
    drivername: Literal['sqlite']

    def sessionmaker(self, db_base_class: type[Base]) -> sessionmaker[Session]:
        url = URL.create(database=self.database, drivername=self.drivername)
        engine = create_engine(url)
        Session = sessionmaker(engine)
        db_base_class.metadata.create_all(engine)

        return Session
