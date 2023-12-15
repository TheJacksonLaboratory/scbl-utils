"""
This module contains functions related to Google Drive that are used in
`main.py` to create a command-line interface.

Functions:
"""
from pathlib import Path
from typing import Any, Hashable, Literal

import gspread as gs
import pandas as pd
from pydantic import field_validator, model_validator
from pydantic.dataclasses import dataclass

from ..db_models.bases import Base


@dataclass
class Sheet:
    worksheet: gs.Worksheet
    db_table: type[Base]
    index_col: str
    col_renamer: dict[str, str]


# TODO: should there be validation in this class? There is validation in
# defaults.py which will be used to validate the worksheet spec, but
# could this class be filled from a different source?
# @dataclass

# TODO: this is not ready

# class Spread:
#     client: gs.Client
#     spreadsheet_url: str # This will be validated by the defaults.py schema. Should it be validated here too?
#     worksheet_specs: dict[str, int | str | dict[str, str | int]] # Same as above
#     db_tables: dict[str, type[Base]] = {model.__tablename__: model for model in Base.__subclasses__()}
#     index_col: str = 'library.id' # validated here, should be validated in defaults.py schema

#     @field_validator('index_col')
#     def check_index_col(self, value: str) -> str:
#         if '.' not in value or value.count('.') != 1:
#             raise ValueError(f'index_col must be of the form <table>.<column>, but {value} was given.')

#         return value

#     @model_validator(mode='after')
#     def index_col_in_db(self):
#         equiv_db_table, column = self.index_col.split('.')

#         if equiv_db_table not in self.db_tables:
#             raise ValueError(f'index_col must be of the form <table>.<column>. <table> must be one of {self.db_tables}, but {equiv_db_table} was given.')

#         if not hasattr(self.db_tables[equiv_db_table], column):
#             raise ValueError(f'index_col must be of the form <table>.<column>. <column> must be one of {self.db_tables[equiv_db_table].__table__.columns}, but {column} was given.')

#         return self

#     def split_combine_worksheets(self) -> dict[str, pd.DataFrame]:
#         """
#         Split the worksheets into tables and combine them into a single
#         dictionary of DataFrames
#         """
#         spread = self.client.open_by_url(self.spreadsheet_url)
#         dfs: list[pd.DataFrame] = []

#         for worksheet_id, worksheet_spec in self.worksheet_specs.items():
#             sheet = spread.get_worksheet_by_id(worksheet_id)
#             values = sheet.get_values(combine_merged_cells=True)
#             header = values[worksheet_spec['head']]
#             data = values[worksheet_spec['head'] + 1:]
#             df = pd.DataFrame(data, columns=header)

#             df.drop(columns=[col for col in df.columns if col not in worksheet_spec['columns']], inplace=True)
#             df.rename(columns=worksheet_spec['columns'], inplace=True)
#             df.set_index(f'{self.equiv_db_table}.{self.index_col}', inplace=True)

#             dfs.append(df)

#         split_combined_dfs = {}
#         for table_name in self.db_tables:
#             table_dfs = []
#             for df in dfs:
#                 columns = [col for col in df.columns if table_name in col]
#                 table_dfs.append(df[columns])
#             split_combined_dfs[table_name] = pd.concat(table_dfs, axis=1)

#         return split_combined_dfs


# client = gs.service_account(filename=Path('/Users/saida/.config/scbl-utils/google-drive/service-account.json'))
