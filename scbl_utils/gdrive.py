from collections.abc import Generator
from functools import cached_property
from re import fullmatch
from typing import Any

import gspread as gs
from pydantic import computed_field

from .config import SpreadsheetConfig, WorksheetConfig
from .pydantic_model_config import StrictBaseModel


class GWorksheet(
    StrictBaseModel, arbitrary_types_allowed=True, frozen=True, strict=True
):
    config: WorksheetConfig
    worksheet: gs.Worksheet

    @computed_field
    @cached_property
    def _split_data(self) -> dict[str, list]:
        return {db_model_name: [] for db_model_name in self.config.db_model_names}

    @computed_field
    @cached_property
    def _raw_values(self) -> list[list[str]]:
        return self.worksheet.get_values(combine_merged_cells=True)

    @computed_field
    @cached_property
    def _all_columns(self) -> list[str]:
        return self._raw_values[self.config.head]

    @computed_field
    @cached_property
    def _formatted_data(
        self,
    ) -> Generator[Generator[str | None, None, None], None, None]:
        all_data = self._raw_values[self.config.head + 1 :]
        return ((val if val != '' else None for val in row) for row in all_data)

    # TODO: optimize this
    @computed_field
    @cached_property
    def as_records(self) -> dict[str, list[Any]]:
        for row in self._formatted_data:
            split_records = {db_model_name: {} for db_model_name in self._split_data}

            for col, val in zip(self._all_columns, row, strict=True):
                if col not in self.config.columns_to_targets:
                    continue

                if col in self.config.empty_means_drop and val is None:
                    break

                target_config = self.config.columns_to_targets[col]

                for target in target_config.targets:
                    db_model_name = target.split('.')[0]
                    rec = split_records[db_model_name]

                    if target not in rec:
                        rec[target] = val
                        continue

                    if f'{target}_1' not in rec:
                        rec[f'{target}_1'] = val
                        continue

                    col_pattern = rf'{target}(_\d+)'
                    matches = (
                        fullmatch(pattern=col_pattern, string=existing_col)
                        for existing_col in rec
                    )
                    latest_duplicate_number = max(
                        match.group(1) for match in matches if match is not None
                    )

                    rec[f'{target}{latest_duplicate_number}'] = val

            for db_model_name, data in self._split_data.items():
                data.append(split_records[db_model_name])

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
