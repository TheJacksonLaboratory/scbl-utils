from collections.abc import Sequence
from functools import cached_property
from pathlib import Path
from typing import Any, Literal

from pydantic import (
    DirectoryPath,
    HttpUrl,
    NonNegativeInt,
    computed_field,
    model_validator,
)

from .pydantic_model_config import StrictBaseModel
from .validated_types import DBTarget


class DBConfig(StrictBaseModel, frozen=True, strict=True):
    database: str
    drivername: Literal['sqlite'] = 'sqlite'


class TargetConfig(StrictBaseModel, frozen=True, strict=True):
    targets: set[DBTarget]
    replace: dict[str, Any]

    @computed_field
    @cached_property
    def db_model_names(self) -> set[str]:
        return {target.split('.')[0] for target in self.targets}


class WorksheetConfig(StrictBaseModel, frozen=True, strict=True):
    replace: dict[str, Any]
    head: NonNegativeInt = 0
    type_converters: dict[str, str]
    empty_means_drop: set[str]
    columns_to_targets: dict[str, TargetConfig]

    @model_validator(mode='after')
    def validate_column_existence(self: 'WorksheetConfig') -> 'WorksheetConfig':
        column_subsets = (
            ('type_converters', self.type_converters.keys()),
            ('empty_means_drop', self.empty_means_drop),
        )

        for subset_key, subset in column_subsets:
            if not subset <= self.columns_to_targets.keys():
                raise ValueError(
                    f'{subset_key} must be a subset of the columns defined as keys of columns_to_targets'
                )

        return self

    @computed_field
    @cached_property
    def db_model_names(self) -> set[str]:
        return {
            model_name
            for target_config in self.columns_to_targets.values()
            for model_name in target_config.db_model_names
        }


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
