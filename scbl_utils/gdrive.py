from collections.abc import Collection
from itertools import groupby
from typing import Any, Literal, Protocol, TypedDict

import polars as pl

from .config import GoogleSpreadsheetConfig, GoogleWorksheetConfig, MergeStrategy
from .pydantic_model_config import StrictBaseModel


class InsertableData(TypedDict):
    columns: list[str]
    data: list[list]


class GoogleApiResource(Protocol):
    """Class just for type-hinting. not implementeed yet"""

    pass


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

    def _subset(
        self, lf: pl.LazyFrame, desired_columns: Collection[str]
    ) -> pl.LazyFrame:
        return lf.select(*desired_columns)

    def _filter(
        self, lf: pl.LazyFrame, empty_means_drop: Collection[str]
    ) -> pl.LazyFrame:
        filtered = lf.filter(pl.all().is_not_null())
        return (
            filtered.drop_nulls(subset=empty_means_drop)
            if empty_means_drop
            else filtered
        )

    def _cast(self, lf: pl.LazyFrame, column_to_type: dict[str, type]) -> pl.LazyFrame:
        return lf.cast(column_to_type)

    def to_lf(self, config: GoogleWorksheetConfig) -> pl.LazyFrame:
        raw_lf = self._to_raw_lf(config.header)
        lf_with_replaced_values = self._replace_values(raw_lf, replace=config.replace)
        desired_lf_subset = self._subset(
            lf_with_replaced_values, desired_columns=config.column_to_targets.keys()
        )
        filtered_lf = self._filter(
            desired_lf_subset, empty_means_drop=config.empty_means_drop
        )
        print(filtered_lf.collect())
        clean_lf = self._cast(filtered_lf, column_to_type=config.column_to_type)

        return clean_lf


class GoogleSheetsResponse(StrictBaseModel, frozen=True, strict=True):
    spreadsheetId: str
    valueRanges: list[GoogleSheetsValueRange]

    def _split_value_range(
        self, value_range: GoogleSheetsValueRange, sheet_config: GoogleWorksheetConfig
    ) -> dict[str, pl.LazyFrame]:
        lfs: dict[str, pl.LazyFrame] = {}
        sheet_as_lf = value_range.to_lf(sheet_config)

        for old_column in sheet_as_lf.columns:
            column_config = sheet_config.column_to_targets[old_column]

            for target in column_config.targets:
                db_model_name = target.split('.')[0]
                lf = lfs.get(db_model_name, pl.LazyFrame())

                column_data_to_append = sheet_as_lf.select(
                    pl.col(old_column).replace(column_config.replace).alias(target)
                )

                lfs[db_model_name] = lf.with_columns(column_data_to_append.collect())

        return lfs

    def _merge_lfs(
        self,
        split_lfs: dict[str, dict[str, pl.LazyFrame]],
        merge_strategies: dict[str, MergeStrategy],
    ) -> dict[str, pl.LazyFrame]:
        merged_lfs = {}

        for db_model_name, strategy in merge_strategies.items():
            lf = None

            for sheet_name in strategy.order:
                other_lf = split_lfs[sheet_name][db_model_name]
                if lf is None:
                    lf = other_lf
                else:
                    lf = lf.join(other_lf, how='left', on=strategy.on)

            merged_lfs[db_model_name] = lf

        return merged_lfs

    def to_lfs(self, config: GoogleSpreadsheetConfig) -> dict[str, pl.LazyFrame]:
        split_lfs: dict[str, dict[str, pl.LazyFrame]] = {}

        for value_range in self.valueRanges:
            sheet_name = value_range.range
            sheet_config = config.worksheet_configs[sheet_name]

            split_lfs[sheet_name] = self._split_value_range(value_range, sheet_config)

        merged_lfs = self._merge_lfs(
            split_lfs, merge_strategies=config.merge_strategies
        )

        return merged_lfs
