from collections.abc import Mapping
from typing import Annotated, Any

from pydantic import HttpUrl, NonNegativeInt, StringConstraints, model_validator

from scbl_utils.data_io_utils import DBTarget

from ..pydantic_model_config import StrictBaseModel


class ColumnToTargets(StrictBaseModel, frozen=True):
    column: str
    targets: set[DBTarget]
    mapper: Mapping[str, Any]


class WorksheetConfig(StrictBaseModel, frozen=True):
    replace: Mapping[str, Any]
    head: NonNegativeInt = 0
    type_converters: Mapping[str, Annotated[str, StringConstraints]]
    empty_means_drop: set[str]
    cols_to_targets: list[ColumnToTargets]

    @model_validator(mode='after')
    def validate_column_existence(self: 'WorksheetConfig') -> 'WorksheetConfig':
        column_subsets = (
            ('type_converters', self.type_converters.keys()),
            ('empty_means_drop', self.empty_means_drop),
        )

        for subset_key, subset in column_subsets:
            if not subset <= {
                col_to_targets.column for col_to_targets in self.cols_to_targets
            }:
                raise ValueError(
                    f'[green]{subset_key}[/] must be a subset of the columns defined in [green]cols_to_targets[/]'
                )

        return self


class SpreadsheetConfig(StrictBaseModel, frozen=True):
    spreadsheet_url: HttpUrl
    worksheet_configs: Mapping[str, WorksheetConfig]
    worksheet_priority: set[str]

    @model_validator(mode='after')
    def validate_worksheet_ids(self: 'SpreadsheetConfig') -> 'SpreadsheetConfig':
        if not self.worksheet_configs.keys() == self.worksheet_priority:
            raise ValueError(f'Workheet priority and worksheet IDs must be the same.')

        return self
