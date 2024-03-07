from collections.abc import Generator
from functools import cached_property
from re import fullmatch
from typing import TypedDict

import gspread as gs
import polars as pl
from pydantic import computed_field

from .config import SpreadsheetConfig, WorksheetConfig
from .pydantic_model_config import StrictBaseModel


class InsertableData(TypedDict):
    columns: list[str]
    data: list[list]


class GWorksheet(
    StrictBaseModel, arbitrary_types_allowed=True, frozen=True, strict=True
):
    config: WorksheetConfig
    worksheet: gs.Worksheet

    @computed_field
    @cached_property
    def _raw_df(self):
        raw_values = self.worksheet.get(
            pad_values=True, maintain_size=True, combine_merged_cells=True
        )

        columns = raw_values[self.config.head]
        data = raw_values[self.config.head + 1 :]

        return pl.DataFrame(schema=columns, data=data)

    @computed_field
    @cached_property
    def _cleaned_df(self):
        df = self._raw_df
        desired_columns = self.config.db_target_configs.values()

        return df.with_columns(pl.all(*desired_columns).replace('', None))

    @computed_field
    @cached_property
    def _split_data(self) -> dict[str, pl.DataFrame]:
        return {
            db_model_name: pl.DataFrame()
            for db_model_name in self.config.db_model_names
        }

    # TODO: optimize this
    @computed_field
    @cached_property
    def as_insertable_data(self) -> dict[str, InsertableData]:
        for i, col in enumerate(self._all_columns):
            if col not in self.config.columns_to_targets:
                continue

            target_config = self.config.columns_to_targets[col]
            for target in target_config.targets:
                db_model_name = target.split('.')[0]
                df = self._split_data[db_model_name]

                df.columns.append(col)
                df[col] = [row for row in self._formated_data]

        for row in self._formatted_data:
            for col, val in zip(self._all_columns, row, strict=True):
                if col not in self.config.columns_to_targets:
                    continue

                if col in self.config.empty_means_drop and val is None:
                    break

                target_config = self.config.columns_to_targets[col]

                for target in target_config.targets:
                    db_model_name = target.split('.')[0]
                    df = self._split_data[db_model_name]

                    if target not in df.columns:
                        df.columns.append(target)
                        df.select(pl.col(target))
                        rec['columns'].append(target)
                        rec['row'].append(val)
                        continue

                    first_duplicate_name = f'{target}_1'
                    if first_duplicate_name not in rec:
                        rec['columns'].append(first_duplicate_name)
                        rec['row'].append(val)
                        continue

                    col_pattern = rf'{target}(_\d+)'
                    matches = (
                        fullmatch(pattern=col_pattern, string=existing_col)
                        for existing_col in rec
                    )
                    latest_duplicate_number = max(
                        match.group(1) for match in matches if match is not None
                    )

                    rec['columns'].append(f'{target}{latest_duplicate_number}')
                    rec['row'].append(val)

            for db_model_name, data_set in self._split_data.items():
                data_set['data'].append(split_records[db_model_name]['row'])

        return self._split_data


class GSpreadsheet(
    StrictBaseModel, arbitrary_types_allowed=True, frozen=True, strict=True
):
    config: SpreadsheetConfig
    gclient: gs.Client

    @computed_field
    @cached_property
    def spreadsheet(self) -> gs.Spreadsheet:
        return self.gclient.open_by_url(str(self.config.spreadsheet_url))

    @computed_field
    @cached_property
    def worksheets(self) -> Generator[GWorksheet, None, None]:
        for worksheet_id, config in self.config.worksheet_configs.items():
            worksheet = self.spreadsheet.get_worksheet_by_id(worksheet_id)

            yield GWorksheet(config=config, worksheet=worksheet)
