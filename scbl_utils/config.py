from pathlib import Path
from typing import Any, Literal

from pydantic import DirectoryPath, NonNegativeInt, model_validator

from .pydantic_model_config import StrictBaseModel
from .validated_types import DBModelName, DBTarget, TypeString


class DBConfig(StrictBaseModel, frozen=True):
    database: str
    drivername: Literal['sqlite'] = 'sqlite'


class GoogleColumnConfig(StrictBaseModel, frozen=True):
    targets: set[DBTarget]
    replace: dict[DBTarget, dict[str, Any]] = {}

    @model_validator(mode='after')
    def validate_replace(self):
        if not self.targets >= self.replace.keys():
            raise ValueError(
                'Targets must be a superset of replacement dictionary keys.'
            )

        return self


class GoogleWorksheetConfig(StrictBaseModel, frozen=True):
    column_to_targets: dict[str, GoogleColumnConfig]
    column_to_type: dict[str, TypeString] = {}
    empty_means_drop: set[str] = set()
    header: NonNegativeInt = 0
    replace: dict[str, Any] = {}
    forward_fill_nulls: list[str] = []

    @model_validator(mode='after')
    def validate_columns(self):
        if not self.column_to_targets.keys() >= self.column_to_type.keys():
            raise ValueError(f'Some helpful error 1')

        return self


class MergeStrategy(StrictBaseModel, frozen=True):
    merge_on: list[DBTarget] | str
    order: list[str]


class GoogleSpreadsheetConfig(StrictBaseModel, frozen=True):
    worksheet_configs: dict[str, GoogleWorksheetConfig]
    merge_strategies: dict[DBModelName, MergeStrategy] = {}

    # TODO: add more validation here. Make sure that all the
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
            raise NotImplementedError(
                'Currently, worksheet names specified in worksheet configurations must be the same as worksheet names in merge strategies.'
            )
            raise ValueError(
                f'Workheet configurations must be a superset of worksheet names in merge strategies.'
            )

        targets_from_configs = set()
        for worksheet_config in self.worksheet_configs.values():
            for column_config in worksheet_config.column_to_targets.values():
                for target in column_config.targets:
                    targets_from_configs.add(target.split('.')[0])

        if targets_from_configs != self.merge_strategies.keys():
            raise NotImplementedError(
                'something informative about merge strategies'
                + str(targets_from_configs)
                + str(self.merge_strategies.keys())
            )

        return self


class SystemConfig(StrictBaseModel, frozen=True):
    delivery_parent_dir: DirectoryPath = Path('/sc/service/delivery/')
