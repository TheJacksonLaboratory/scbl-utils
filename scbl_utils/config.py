from pathlib import Path
from typing import Any, Literal

from pydantic import DirectoryPath, NonNegativeInt, model_validator

from .pydantic_model_config import StrictBaseModel
from .validated_types import DBModelName, DBTarget, TypeString


class DBConfig(StrictBaseModel, frozen=True, strict=True):
    database: str
    drivername: Literal['sqlite'] = 'sqlite'


class GoogleColumnConfig(StrictBaseModel, frozen=True, strict=True):
    targets: set[DBTarget]
    replace: dict[DBTarget, dict[str, Any]] = {}

    @model_validator(mode='after')
    def validate_replace(self):
        if not self.targets >= self.replace.keys():
            raise ValueError

        return self


class GoogleWorksheetConfig(StrictBaseModel, frozen=True, strict=True):
    column_to_targets: dict[str, GoogleColumnConfig]
    column_to_type: dict[str, TypeString] = {}
    empty_means_drop: set[str] = set()
    header: NonNegativeInt = 0
    index_col: str | None = None
    replace: dict[str, Any] = {}

    @model_validator(mode='after')
    def validate_columns(self):
        if not self.column_to_targets.keys() >= self.column_to_type.keys():
            raise ValueError(f'Some helpful error')

        return self


class MergeStrategy(StrictBaseModel, frozen=True, strict=True):
    on: list[DBTarget] | str
    order: list[str]


class GoogleSpreadsheetConfig(StrictBaseModel, frozen=True, strict=True):
    spreadsheet_id: str
    worksheet_configs: dict[str, GoogleWorksheetConfig]
    merge_strategies: dict[DBModelName, MergeStrategy] = {}

    @model_validator(mode='after')
    def validate_worksheet_ids(
        self: 'GoogleSpreadsheetConfig',
    ) -> 'GoogleSpreadsheetConfig':
        worksheet_names_from_config = self.worksheet_configs.keys()
        worksheet_names_from_merge_strategies = {
            worksheet_name
            for merge_strategy in self.merge_strategies.values()
            for worksheet_name in merge_strategy.order
        }

        if worksheet_names_from_config != worksheet_names_from_merge_strategies:
            raise ValueError(
                f'Workheet configurations must be a superset of worksheet names in merge strategies.'
            )

        return self


class SystemConfig(StrictBaseModel, frozen=True, strict=True):
    delivery_parent_dir: DirectoryPath = Path('/sc/service/delivery/')
