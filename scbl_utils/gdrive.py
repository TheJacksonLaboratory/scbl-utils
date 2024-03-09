from collections.abc import Collection
from typing import Any, Literal, Protocol, TypedDict

import polars as pl

from .config import GoogleSpreadsheetConfig, GoogleWorksheetConfig
from .pydantic_model_config import StrictBaseModel


class InsertableData(TypedDict):
    columns: list[str]
    data: list[list]


class GoogleApiResource(Protocol):
    """Class just for type-hinting. not implementeed yet"""

    pass


# TODO: switch to lazyframes when I figure it out
class GoogleSheetsValueRange(StrictBaseModel, frozen=True, strict=True):
    range: str
    majorDimension: Literal['ROWS']
    values: list[list[str]]

    def _to_raw_lf(self, header: int) -> pl.LazyFrame:
        columns = self.values[header]
        data = self.values[header + 1 :]

        return pl.LazyFrame(schema=columns, data=data, orient='row')

    def _replace_values(
        self, lf: pl.LazyFrame, replace: dict[str, Any]
    ) -> pl.LazyFrame:
        return lf.with_columns(pl.all().replace(replace))

    def _filter(
        self, lf: pl.LazyFrame, empty_means_drop: Collection[str]
    ) -> pl.LazyFrame:
        return lf.filter(pl.all_horizontal(pl.all().is_null())).drop_nulls(
            subset=empty_means_drop
        )

    def _subset(
        self, lf: pl.LazyFrame, desired_columns: Collection[str]
    ) -> pl.LazyFrame:
        return lf.select(*desired_columns)

    def _cast(self, lf: pl.LazyFrame, column_to_type: dict[str, type]) -> pl.LazyFrame:
        return lf.cast(column_to_type)

    def to_lf(self, config: GoogleWorksheetConfig) -> pl.LazyFrame:
        raw_lf = self._to_raw_lf(config.header)
        lf_with_replaced_values = self._replace_values(raw_lf, replace=config.replace)
        desired_lf_subset = self._subset(
            lf_with_replaced_values, desired_columns=config.column_to_targets.keys()
        )
        cleaned_lf = self._cast(desired_lf_subset, column_to_type=config.column_to_type)

        return cleaned_lf


class GoogleSheetsResponse(StrictBaseModel, frozen=True, strict=True):
    spreadsheetId: str
    valueRanges: list[GoogleSheetsValueRange]

    def to_lfs(self, config: GoogleSpreadsheetConfig) -> dict[str, pl.LazyFrame]:
        lfs: dict[str, pl.LazyFrame] = {}
        sources: dict[str, str] = {}

        for value_range in self.valueRanges:
            sheet_name = value_range.range
            sheet_config = config.worksheet_configs[sheet_name]

            sheet_as_lf = value_range.to_lf(sheet_config)

            for old_column in sheet_as_lf.columns:
                column_config = sheet_config.column_to_targets[old_column]

                for target in column_config.targets:
                    db_model_name = target.split('.')[0]
                    lf = lfs.get(db_model_name, pl.LazyFrame())

                    # if target not in lf.columns:
                    #     column_data_to_append = sheet_as_lf.select(
                    #         pl.col(old_column).replace(replace).alias(target)
                    #     )

                    #     lfs[db_model_name] = lf.with_columns(
                    #         column_data_to_append.collect()
                    #     )

                    #     continue
                    # elif target not in config.merge_order:

                    # # prior_data_source = sources[target]
                    # merge_priority = config.merge_priority[target]

                    # # if merge_priority.index(prior_data_source) < merge_priority.index(sheet_name):

                    # elif sources[target]:

                    #     i = 0
                    #     new_column = f'{target}_{i}'
                    #     while new_column in lf.columns:
                    #         i += 1
                    #         new_column = f'{target}_{i}'

                    # replace = column_config.replace.get(target, {})

        return lfs
