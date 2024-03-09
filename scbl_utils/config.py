from pathlib import Path
from typing import Any, Literal

from pydantic import DirectoryPath, NonNegativeInt, field_validator, model_validator

from .pydantic_model_config import StrictBaseModel
from .validated_types import DBTarget, TypeString


class DBConfig(StrictBaseModel, frozen=True, strict=True):
    database: str
    drivername: Literal['sqlite'] = 'sqlite'


class GoogleColumnConfig(StrictBaseModel, frozen=True, strict=True):
    targets: set[DBTarget]
    replace: dict[DBTarget, dict[str, Any]] = {}

    @model_validator(mode='after')
    def validate_replace(self):
        if self.replace.keys() > self.targets:
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
        if self.column_to_type.keys() > self.column_to_targets.keys():
            raise ValueError(f'Some helpful error')

        return self


class GoogleSpreadsheetConfig(StrictBaseModel, frozen=True, strict=True):
    spreadsheet_id: str
    worksheet_configs: dict[str, GoogleWorksheetConfig]
    merge_order: dict[DBTarget, list[str]] = {}

    @model_validator(mode='after')
    def validate_worksheet_ids(
        self: 'GoogleSpreadsheetConfig',
    ) -> 'GoogleSpreadsheetConfig':
        if self.worksheet_configs.keys() < {
            worksheet_id
            for worksheet_id_list in self.merge_priority.values()
            for worksheet_id in worksheet_id_list
        }:
            raise ValueError(
                f'Workheet merge priority and worksheet IDs must be the same.'
            )

        return self


class SystemConfig(StrictBaseModel, frozen=True, strict=True):
    delivery_parent_dir: DirectoryPath = Path('/sc/service/delivery/')
