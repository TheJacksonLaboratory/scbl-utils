"""
This module contains functions related to Google Drive that are used in
`main.py` to create a command-line interface.

Functions:
"""
from pathlib import Path
from typing import Any, Collection, Hashable, Literal

import gspread as gs
import pandas as pd
from numpy import nan
from pydantic import ConfigDict, Field, field_validator, model_validator
from pydantic.dataclasses import dataclass

from ..db_models.bases import Base

sheet_config = ConfigDict(arbitrary_types_allowed=True)


@dataclass(config=sheet_config)
class Sheet:
    worksheet: gs.Worksheet
    index_col: str
    required_cols: list[str]
    col_renamer: dict[str, str] = Field(default_factory=dict)
    none_values: Collection[str] = Field(default_factory=list)
    type_converters: dict[str, str] = Field(default_factory=dict)
    head: int = 0
    cols_to_add: dict[str, Any] = Field(
        default_factory=dict
    )  # TODO: make this a separate class with its own validation

    # TODO: The same set difference and error message is calculated 3 times. Fix that
    @model_validator(mode='after')
    def check_column_matching(self):
        if not self.col_renamer:
            return self

        if self.index_col not in self.col_renamer.values():
            raise ValueError(
                f'index_col {self.index_col} must be in the values of col_renamer.'
            )

        if self.type_converters.keys() - set(self.col_renamer.values()):
            raise ValueError(
                f'The keys of type_converters must be a subset of the values of col_renamer. The following are the keys of type_converters that are not in the values of col_renamer:\n{self.type_converters.keys() - set(self.col_renamer.values())}'
            )

        if any(
            spec['from'] not in self.col_renamer.values()
            for spec in self.cols_to_add.values()
        ):
            raise ValueError('something is wrong')

        if set(self.required_cols) - set(self.col_renamer.values()):
            raise ValueError(
                f'The columns specified in required_cols ({self.required_cols}) must be in the values of col_renamer.'
            )

        return self

    def to_df(self) -> pd.DataFrame:
        # Get the data out of the sheet, assigning header_row and data
        values = self.worksheet.get_values(
            combine_merged_cells=True, value_render_option='UNFORMATTED_VALUE'
        )
        header = values[self.head]
        data = values[self.head + 1 :]

        df = pd.DataFrame(data, columns=header)

        # Strip whitespace
        for col, series in df.select_dtypes(include='object').items():
            df[col] = series.str.strip()

        # Replace none_values with None and TRUE/FALSE with True/False
        replacement_dict = {none_value: None for none_value in self.none_values} | {
            'TRUE': True,
            'FALSE': False,
        }
        df.replace(replacement_dict, inplace=True)

        # Rename columns and convert types
        df.rename(columns=self.col_renamer, inplace=True)
        df = df.astype(self.type_converters)

        # Set index and drop blank rows
        df.set_index(self.index_col, inplace=True)
        df.dropna(subset=self.required_cols, inplace=True, how='any')

        # Create new columns from old columns based passed-in mapping
        for new_col, replacement_spec in self.cols_to_add.items():
            old_col = replacement_spec['from']
            df[new_col] = df[old_col].replace(replacement_spec['mapper'])

        return df

    def to_records(self) -> list[dict[Hashable, Any]]:
        return self.to_df().reset_index(names=self.index_col).to_dict(orient='records')
