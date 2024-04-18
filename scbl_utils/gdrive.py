from collections.abc import Collection
from typing import Any, Literal

import polars as pl

from .config import GoogleSpreadsheetConfig, GoogleWorksheetConfig, MergeStrategy
from .pydantic_model_config import StrictBaseModel


class GoogleSheetsValueRange(StrictBaseModel, frozen=True, strict=True):
    range: str
    majorDimension: Literal['ROWS']
    values: list[list[str]]

    def _to_raw_lf(self, header: int) -> pl.DataFrame:
        row_length = max(len(row) for row in self.values)
        normalized_length_values = [
            row + [''] * (row_length - len(row)) for row in self.values
        ]

        columns = normalized_length_values[header]
        data = normalized_length_values[header + 1 :]

        return pl.DataFrame(schema=columns, data=data, orient='row')

    def _replace_values(
        self, lf: pl.DataFrame, replace: dict[str, Any]
    ) -> pl.DataFrame:
        return lf.with_columns(pl.all().replace(replace))

    def _subset(
        self, lf: pl.DataFrame, desired_columns: Collection[str]
    ) -> pl.DataFrame:
        return lf.select(*desired_columns)

    def _filter(
        self, lf: pl.DataFrame, empty_means_drop: Collection[str]
    ) -> pl.DataFrame:
        filtered = lf.filter(~pl.all_horizontal(pl.all().is_null()))
        return (
            filtered.drop_nulls(subset=empty_means_drop)
            if empty_means_drop
            else filtered
        )

    def _cast(self, lf: pl.DataFrame, column_to_type: dict[str, type]) -> pl.DataFrame:
        for col, type_ in column_to_type.items():
            if type_ == int:
                lf = lf.with_columns(pl.col(col).str.replace_all(',', ''))

        return lf.cast(column_to_type)

    def _forward_fill(
        self, lf: pl.DataFrame, columns_to_fill: list[str]
    ) -> pl.DataFrame:
        return lf.with_columns(pl.col(columns_to_fill).forward_fill())

    def to_lf(self, config: GoogleWorksheetConfig) -> pl.DataFrame:
        raw_lf = self._to_raw_lf(config.header)
        lf_with_replaced_values = self._replace_values(raw_lf, replace=config.replace)
        desired_lf_subset = self._subset(
            lf_with_replaced_values, desired_columns=config.column_to_targets.keys()
        )
        filtered_lf = self._filter(
            desired_lf_subset, empty_means_drop=config.empty_means_drop
        )
        clean_lf = self._cast(filtered_lf, column_to_type=config.column_to_type)
        filled_lf = self._forward_fill(clean_lf, config.forward_fill_nulls)

        return filled_lf


class GoogleSheetsResponse(StrictBaseModel, frozen=True, strict=True):
    spreadsheetId: str
    valueRanges: list[GoogleSheetsValueRange]

    def _split_value_range(
        self, value_range: GoogleSheetsValueRange, sheet_config: GoogleWorksheetConfig
    ) -> dict[str, pl.DataFrame]:
        lfs: dict[str, pl.DataFrame] = {}
        sheet_as_lf = value_range.to_lf(sheet_config)

        for old_column in sheet_as_lf.columns:
            column_config = sheet_config.column_to_targets[old_column]

            for target in column_config.targets:
                db_model_name = target.split('.')[0]
                lf = lfs.get(db_model_name, pl.DataFrame())

                column_data_to_append = sheet_as_lf.select(
                    pl.col(old_column)
                    .alias(target)
                    .replace(column_config.replace.get(target, {}))
                )

                lfs[db_model_name] = lf.with_columns(column_data_to_append)

        return lfs

    def _merge_lfs(
        self,
        split_lfs: dict[str, dict[str, pl.DataFrame]],
        merge_strategies: dict[str, MergeStrategy],
    ) -> dict[str, pl.DataFrame]:
        merged_dfs = {}
        # TODO: can this be factored out even further?
        for db_model_name, strategy in merge_strategies.items():
            df = None

            for sheet_name in strategy.order:
                right_df = split_lfs[sheet_name][db_model_name]

                if df is None:
                    df = right_df
                    continue

                suffix = '_right'
                df = df.join(
                    right_df, how='outer_coalesce', on=strategy.merge_on, suffix=suffix
                )

                duplicate_columns = (
                    (col.removesuffix(suffix), col)
                    for col in df.columns
                    if col.endswith(suffix)
                )
                for left_column, right_column in duplicate_columns:
                    left_series = df.get_column(left_column)
                    right_series = df.get_column(right_column)

                    left_filled = left_series.fill_null(right_series)

                    df = df.with_columns(left_filled).drop(right_column)

            if df is not None:
                merged_dfs[db_model_name] = df

        return merged_dfs

    def to_dfs(self, config: GoogleSpreadsheetConfig) -> dict[str, pl.DataFrame]:
        split_lfs: dict[str, dict[str, pl.DataFrame]] = {}

        for value_range in self.valueRanges:
            sheet_name = value_range.range.split('!')[0].strip("'")
            sheet_config = config.worksheet_configs[sheet_name]

            split_lfs[sheet_name] = self._split_value_range(value_range, sheet_config)

        merged_dfs = self._merge_lfs(
            split_lfs, merge_strategies=config.merge_strategies
        )

        return merged_dfs
