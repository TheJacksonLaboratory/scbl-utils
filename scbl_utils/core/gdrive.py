"""
This module contains functions related to Google Drive that are used in
`main.py` to create a command-line interface.

Functions:
"""
from collections.abc import Collection, Hashable
from pathlib import Path
from re import match
from typing import Any, Literal

import gspread as gs
import pandas as pd
from numpy import nan
from pydantic import ConfigDict, Field, field_validator, model_validator
from pydantic.dataclasses import dataclass
from rich import print as rprint
from sqlalchemy import inspect
from typer import Abort

from ..db_models import data, definitions
from ..db_models.bases import Base
from ..defaults import OBJECT_SEP_CHAR
from .validation import valid_db_target

sheet_config = ConfigDict(arbitrary_types_allowed=True)


@dataclass(config=sheet_config)
class TrackingSheet:
    worksheet: gs.Worksheet
    index_col: str
    empty_means_drop: list[str]
    cols_to_targets: Collection[
        dict[str, str | Collection[str] | dict[str, str]]
    ] = Field(default_factory=list)
    replace: dict[str, Any] = Field(default_factory=dict)
    type_converters: dict[str, str] = Field(default_factory=dict)
    head: int = 0

    @field_validator('cols_to_targets')
    @classmethod
    def check_cols_to_targets(
        cls,
        cols_to_targets: Collection[dict[str, str | Collection[str] | dict[str, str]]],
    ):
        invalid_db_targets = {
            col_dict['from']: {
                target for target in col_dict['to'] if not valid_db_target(target)
            }
            for col_dict in cols_to_targets
        }

        # TODO: informative error messages
        if any(invalid_db_targets.values()):
            rprint(
                'something about columns',
                *(
                    f'[orange]{sheet_column}[/]: [orange1]{invalid_targets}[/]'
                    for sheet_column, invalid_targets in invalid_db_targets.items()
                ),
                sep='\n',
            )
            raise Abort()

        return cols_to_targets

    @model_validator(mode='after')
    def check_column_matching(self):
        if not self.cols_to_targets:
            return self

        sheet_columns = {
            col_conversion['from']
            for col_conversion in self.cols_to_targets
            if isinstance(col_conversion['from'], str)
        }
        column_collections = {
            'empty_means_drop': set(self.empty_means_drop),
            'type_converters': set(self.type_converters.keys()),
            'index_col': {self.index_col},
        }
        not_subsets = [
            f'[green]{name}[/]: [orange1]{collection - sheet_columns}[/]'
            for name, collection in column_collections.items()
            if not collection.issubset(sheet_columns)
        ]

        if not_subsets:
            rprint(
                f'The attributes [green]{column_collections.keys()}[/] specified in [orange1]gdrive-spec.yml[/] must each be a subset of [green]cols_to_targets[/]. The following items in each attribute are not:\n',
                *not_subsets,
                sep='\n',
            )
            raise Abort()

        return self

    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """_summary_

        :param df: _description_
        :type df: pd.DataFrame
        :param replace: _description_
        :type replace: dict[str, Any]
        :param empty_means_drop: _description_
        :type empty_means_drop: list[str]
        :param type_converters: _description_
        :type type_converters: dict[str, str]
        :return: _description_
        :rtype: pd.DataFrame
        """
        cleaned_df = df.copy()
        # Strip whitespace
        for col, series in df.select_dtypes(include='object').items():
            cleaned_df[col] = series.str.strip()

        # Replace values case-insensitively and convert types
        replace = {rf'(?i)^{key}$': val for key, val in self.replace.items()}
        cleaned_df.replace(regex=replace, inplace=True)

        # Drop rows that are empty by the criteria specified in
        # empty_means_drop
        cleaned_df.dropna(subset=self.empty_means_drop, inplace=True, how='any')

        # Convert types
        cleaned_df = cleaned_df.astype(self.type_converters)
        return cleaned_df

    def _split_combine_df(self, df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """_summary_

        :param df: _description_
        :type df: pd.DataFrame
        :param cols_to_targets: _description_
        :type cols_to_targets: Collection[dict[str, str  |  Collection[str]  |  dict[str, str]]]
        :return: _description_
        :rtype: dict[str, pd.DataFrame]
        """
        # Divide the dataframe into db tables
        db_tables = {
            target.split(OBJECT_SEP_CHAR)[0]
            for col_conversion in self.cols_to_targets
            for target in col_conversion['to']
        }
        dfs = {tablename: pd.DataFrame() for tablename in db_tables}

        # Add the columns to the appropriate table and replace values
        # as specified
        for col_conversion in self.cols_to_targets:
            column = col_conversion['from']
            target_list = col_conversion['to']
            mapper = col_conversion.get('mapper', {})

            for target in target_list:
                tablename = target.split(OBJECT_SEP_CHAR)[0]
                df = dfs[tablename]

                cleaned_data_column = df[column].replace(mapper)

                if target in df.columns:
                    df[f'{target}_1'] = cleaned_data_column.copy()
                else:
                    df[target] = cleaned_data_column.copy()

        # TODO: variable names here are bad and this isn't really clean
        for tablename, df in dfs.items():
            col_contains_suffix = df.columns.str.fullmatch(r'.+_\d+')

            if not col_contains_suffix.any():
                continue

            cols_to_append = df.columns[col_contains_suffix]
            renamed_cols_to_append = cols_to_append.str.replace(
                pat=r'_\d+', repl=r'', regex=True
            )

            cols_to_fill = df.columns[
                (~df.columns.isin(renamed_cols_to_append)) & (~col_contains_suffix)
            ]

            dummy_data = {col: [None] * len(df) for col in cols_to_fill}
            dummy_df = pd.DataFrame(dummy_data)

            rows_to_append = pd.DataFrame()
            rows_to_append[renamed_cols_to_append] = df[cols_to_append].copy()
            rows_to_append[cols_to_fill] = dummy_df.copy()

            dfs[tablename] = pd.concat(
                [df[df.columns[~col_contains_suffix]], rows_to_append],
                axis=0,
                ignore_index=True,
            )

        return dfs

    def _assign_first_last_names(
        self, dfs: dict[str, pd.DataFrame]
    ) -> dict[str, pd.DataFrame]:
        """_summary_

        :param dfs: _description_
        :type dfs: dict[str, pd.DataFrame]
        :return: _description_
        :rtype: dict[str, pd.DataFrame]
        """
        person_columns = ['person', 'lab.pi', 'data_set.submitter', 'project.people']
        for col in person_columns:
            tablename = col.split(OBJECT_SEP_CHAR)[0]
            df = dfs[tablename]
            first_name_col, last_name_col, email_col = (
                f'{col}.{suffix}' for suffix in ('first_name', 'last_name', 'email')
            )
            df[[first_name_col, last_name_col]] = df[f'{col}.name'].str.split(
                n=1, expand=True
            )

            if email_col in df.columns:
                df[email_col] = df[[last_name_col, email_col]].agg(
                    func=lambda row: row[email_col]
                    if row[last_name_col] in str(row[email_col])
                    else None,
                    axis=1,
                )

        return dfs

    def to_dfs(self) -> dict[str, pd.DataFrame]:
        # Get the data out of the sheet, assigning header_row and data
        values = self.worksheet.get_values(
            combine_merged_cells=True,  # value_render_option='UNFORMATTED_VALUE' # TODO: this argument messes up dates
        )
        header = values[self.head]
        data = values[self.head + 1 :]

        whole_df = self._clean_df(pd.DataFrame(data, columns=header))
        dfs = self._split_combine_df(whole_df)
        return self._assign_first_last_names(dfs)
