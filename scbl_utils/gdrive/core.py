from collections.abc import Collection
from re import match
from typing import Any

import gspread as gs
import pandas as pd
from numpy import nan
from pydantic import ConfigDict, Field, field_validator, model_validator
from pydantic.dataclasses import dataclass

sheet_config = ConfigDict(arbitrary_types_allowed=True)


@dataclass(config=sheet_config)
class TrackingSheet:
    worksheet: gs.Worksheet
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
        return cols_to_targets

    @model_validator(mode='after')
    def check_column_matching(self):
        return self

    def clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
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
        # Strip whitespace
        for col, series in df.select_dtypes(include='object').items():
            df[col] = series.str.strip()

        # Replace values case-insensitively and convert types
        replace = {rf'(?i)^{key}$': val for key, val in self.replace.items()}
        df.replace(regex=replace, inplace=True)

        # Drop rows that are empty by the criteria specified in
        # empty_means_drop
        df.dropna(subset=self.empty_means_drop, inplace=True, how='any')

        # Convert types
        df = df.astype(self.type_converters)
        return df

    def split_combine_df(self, whole_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
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
            target.split('.')[0]
            for col_conversion in self.cols_to_targets
            for target in col_conversion['to']
        }
        dfs = {tablename: pd.DataFrame(columns=[]) for tablename in db_tables}

        # Add the columns to the appropriate table and replace values
        # as specified
        for col_conversion in self.cols_to_targets:
            column = col_conversion['from']
            target_list = col_conversion['to']
            mapper = col_conversion.get('mapper', {})

            column_with_numeric_suffix_pattern = r'^(?:\w+\.)+_(\d+)$'
            # This could be factored out into a separate function
            for target in target_list:
                tablename = target.split('.')[0]
                target_df = dfs[tablename]

                cleaned_data_column = whole_df[column].replace(mapper)

                duplicate_target_columns = target_df.columns[
                    target_df.columns.str.match(rf'^{target}')
                ]
                latest_duplicate = duplicate_target_columns.max()

                if pd.isna(latest_duplicate):
                    target_df[target] = cleaned_data_column.copy()
                    continue

                latest_duplicate: str

                if match_obj := match(
                    pattern=column_with_numeric_suffix_pattern, string=latest_duplicate
                ):
                    next_duplicate_number = int(match_obj.group(1)) + 1
                    target_df[
                        f'{target}_{next_duplicate_number}'
                    ] = cleaned_data_column.copy()
                else:
                    target_df[f'{target}_1'] = cleaned_data_column.copy()

        # TODO: variable names here are bad and this isn't really clean
        # also might be able to take this into a separate function
        for tablename, df in dfs.items():
            column_contains_suffix = df.columns.str.fullmatch(r'.+_\d+')

            if not column_contains_suffix.any():
                continue

            columns_to_append = df.columns[column_contains_suffix]
            renamed_cols_to_append = columns_to_append.str.replace(
                pat=r'_\d+$', repl=r'', regex=True
            )

            columns_to_fill = df.columns[
                (~df.columns.isin(renamed_cols_to_append)) & (~column_contains_suffix)
            ]

            dummy_data = {col: [None] * len(df) for col in columns_to_fill}
            dummy_df = pd.DataFrame(dummy_data)

            rows_to_append = pd.DataFrame()
            rows_to_append[renamed_cols_to_append] = df[columns_to_append].copy()
            rows_to_append[columns_to_fill] = dummy_df.copy()

            dfs[tablename] = pd.concat(
                [df[df.columns[~column_contains_suffix]], rows_to_append],
                axis=0,
                ignore_index=True,
            )

        return dfs

    def to_dfs(self) -> dict[str, pd.DataFrame]:
        # Get the data out of the sheet, assigning header_row and data
        values = self.worksheet.get_values(
            combine_merged_cells=True,
        )
        header = values[self.head]
        data = values[self.head + 1 :]

        whole_df = self.clean_df(pd.DataFrame(data, columns=header))
        return self.split_combine_df(whole_df)
