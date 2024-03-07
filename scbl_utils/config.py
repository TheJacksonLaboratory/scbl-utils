from collections.abc import Sequence
from functools import cached_property
from pathlib import Path
from typing import Any, Literal

from pydantic import DirectoryPath, HttpUrl, NonNegativeInt, model_validator

from .pydantic_model_config import StrictBaseModel
from .validated_types import DBModelName, DBTarget


class DBConfig(StrictBaseModel, frozen=True, strict=True):
    database: str
    drivername: Literal['sqlite'] = 'sqlite'


class TargetConfig(StrictBaseModel, frozen=True, strict=True):
    target_from_columns: dict[DBTarget, set[str]]
    replace: dict[str, Any]


class WorksheetConfig(StrictBaseModel, frozen=True, strict=True):
    replace: dict[str, Any]
    head: NonNegativeInt = 0
    type_converters: dict[str, str]
    empty_means_drop: set[str]
    db_target_configs: dict[DBModelName, TargetConfig]

    @model_validator(mode='after')
    def validate_column_existence(self: 'WorksheetConfig') -> 'WorksheetConfig':
        column_subsets = (
            ('type_converters', self.type_converters.keys()),
            ('empty_means_drop', self.empty_means_drop),
        )

        all_sheet_columns = set()
        for target_config in self.db_target_configs.values():
            all_sheet_columns |= set(target_config.target_from_columns.values())

        for subset_key, subset in column_subsets:
            if not subset <= all_sheet_columns:
                raise ValueError(
                    f'{subset_key} must be a subset of the columns defined in db_target_config'
                )

        return self


class SpreadsheetConfig(StrictBaseModel, frozen=True, strict=True):
    spreadsheet_url: HttpUrl
    worksheet_configs: dict[str, WorksheetConfig]
    worksheet_priority: Sequence[str]

    @model_validator(mode='after')
    def validate_worksheet_ids(self: 'SpreadsheetConfig') -> 'SpreadsheetConfig':
        if not self.worksheet_configs.keys() == set(self.worksheet_priority):
            raise ValueError(f'Workheet priority and worksheet IDs must be the same.')

        return self


class SystemConfig(StrictBaseModel, frozen=True, strict=True):
    delivery_parent_dir: DirectoryPath = Path('/sc/service/delivery/')
