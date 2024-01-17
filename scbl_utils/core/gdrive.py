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

from ..db_models.bases import Base
from ..defaults import OBJECT_SEP_CHAR

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
        tables = set()
        for col_dict in cols_to_targets:
            for target in col_dict['to']:
                tables.add(target.split(OBJECT_SEP_CHAR)[0])
        not_in_db = {f'[orange1]{t}' for t in (tables - Base.metadata.tables.keys())}

        if not_in_db:
            rprint(
                f'The targets in [green]cols_to_targets[/] specified in [orange1]gdrive-spec.yml[/] must be tables in the database. The following tables are not:\n',
                *not_in_db,
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

    def to_dfs(self) -> dict[str, pd.DataFrame]:
        # Get the data out of the sheet, assigning header_row and data
        values = self.worksheet.get_values(
            combine_merged_cells=True,  # value_render_option='UNFORMATTED_VALUE' # TODO: this argument messes up dates
        )
        header = values[self.head]
        data = values[self.head + 1 :]

        whole_df = pd.DataFrame(data, columns=header)

        # Strip whitespace
        for col, series in whole_df.select_dtypes(include='object').items():
            whole_df[col] = series.str.strip()

        # Replace values case-insensitively and convert types
        replace = {rf'(?i)^{key}$': val for key, val in self.replace.items()}
        whole_df.replace(regex=replace, inplace=True)

        # Drop rows that are empty by the criteria specified in
        # empty_means_drop
        whole_df.dropna(subset=self.empty_means_drop, inplace=True, how='any')

        # Convert types
        whole_df = whole_df.astype(self.type_converters)

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
                # TODO: you are here. you need to figure out how to either make a new column in the dataframe or add these values to an existing column

        # TODO: some additional validation is needed to make sure the following keys actually exist
        # This can probably be dynamically done
        person_columns = ['person', 'lab.pi', 'data_set.submitter', 'project.people']
        for col in person_columns:
            tablename = col.split(OBJECT_SEP_CHAR)[0]
            df = dfs[tablename]
            first_name_col, last_name_col = (
                f'{col}.{suffix}' for suffix in ('first_name', 'last_name')
            )
            df[[first_name_col, last_name_col]] = df[f'{col}.name'].str.split(
                n=1, expand=True
            )

        for df in dfs.values():
            print(df)
        quit()

        return dfs
