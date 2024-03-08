from collections.abc import Collection
from typing import Literal, Protocol, TypedDict

import polars as pl
import polars.selectors as cs

from .config import GoogleSpreadsheetConfig, GoogleWorksheetConfig
from .pydantic_model_config import StrictBaseModel


class InsertableData(TypedDict):
    columns: list[str]
    data: list[list]


class GoogleApiResource(Protocol):
    """Class just for type-hinting. not implementeed yet"""

    pass


# TODO: switch to lazyframes when I figure it out
class _GoogleSheetValueRange(StrictBaseModel, frozen=True, strict=True):
    range: str
    majorDimension: Literal['ROWS']
    values: list[list[str]]

    def _to_raw_lf(self, header: int) -> pl.LazyFrame:
        columns = self.values[header]
        data = self.values[header + 1 :]

        return pl.LazyFrame(schema=columns, data=data, orient='row')

    def _to_clean_lf(
        self,
        raw_lf: pl.LazyFrame,
        desired_columns: Collection[str],
        empty_means_drop: Collection[str],
    ):
        cleaned_lf = raw_lf.select(desired_columns).with_columns(
            pl.col(desired_columns).replace('', None)
        )

        return (
            cleaned_lf.drop_nulls(subset=empty_means_drop)
            if empty_means_drop
            else cleaned_lf.drop_nulls()
        )

    def to_lf(self, config: GoogleWorksheetConfig) -> pl.LazyFrame:
        raw_lf = self._to_raw_lf(config.header)
        cleaned_lf = self._to_clean_lf(
            raw_lf,
            desired_columns=config.column_to_targets.keys(),
            empty_means_drop=config.empty_means_drop,
        )
        return cleaned_lf.cast(config.column_to_type)


class GoogleSheetResponse(StrictBaseModel, frozen=True, strict=True):
    spreadsheetId: str
    valueRanges: list[_GoogleSheetValueRange]

    def to_lfs(self, config: GoogleSpreadsheetConfig) -> dict[str, pl.LazyFrame]:
        lfs: dict[str, pl.LazyFrame] = {}

        for value_range in self.valueRanges:
            sheet_name = value_range.range
            sheet_config = config.worksheet_configs[sheet_name]

            sheet_as_lf = value_range.to_lf(sheet_config)

            for old_column in sheet_as_lf.columns:
                column_config = sheet_config.column_to_targets[old_column]

                for target in column_config.targets:
                    db_model_name = target.split('.')[0]
                    lf = lfs.get(db_model_name, pl.LazyFrame())

                    if target not in lf.columns:
                        new_column = target
                    else:
                        i = 0
                        while f'{target}_{i}' in lf.columns:
                            i += 1

                        new_column = f'{target}_{i}'

                    replace = column_config.replace.get(target, {})

                    column_data_to_append = sheet_as_lf.select(
                        pl.col(old_column).replace(replace).alias(new_column)
                    )

                    lfs[db_model_name] = lf.with_columns(
                        column_data_to_append.collect()
                    )

        return lfs
