from pathlib import Path

import gspread as gs
import pandas as pd
from rich import print as rprint

from scbl_utils.utils.defaults import TRACKING_DF_INDEX_COL


def login(*args, **kwargs) -> gs.Client:  # type: ignore
    """Log into Google Drive and return gspread.Client.

    :raises RuntimeError: If login impossible, raise error
    :return: The logged in client
    :rtype: gs.Client
    """
    from typer import Abort

    from .defaults import DOCUMENTATION

    try:
        return gs.service_account(*args, **kwargs)
    except Exception as e:
        rprint(
            f'Could not log into Google Drive. See {DOCUMENTATION} for instructions on authentication. {e}'
        )
        Abort()


def load_specs(config_files: dict[str, Path]) -> tuple[dict, dict]:
    """Load specifications from google-drive config directory

    :param config_files: A dict mapping the name of a config file to its path
    :type config_files: dict[str, Path]
    :return: The tracking sheet specification and the metrics sheet specification, as a tuple in that order
    :rtype: tuple[dict, dict]
    """
    from yaml import Loader, load

    # Load in the two specification files that instruct script how to
    # get information from Google Drive
    specs = {
        filename: load(path.read_text(), Loader)
        for filename, path in config_files.items()
        if 'spec.yml' in filename
    }

    return specs['trackingsheet-spec.yml'], specs['metricssheet-spec.yml']


class GSheet(gs.Spreadsheet):
    """Inherits from gspread.Spreadsheet, adding a to_df method. Constructor requires same args as gspread.SpreadSheet"""

    def to_df(
        self, sheet_idx: int = 0, col_renaming: dict[str, str] = {}, header_row: int = 0, index_col: str = TRACKING_DF_INDEX_COL
    ) -> pd.DataFrame:
        # Get table and assign data and header row, then convert to df
        table = self.get_worksheet(sheet_idx).get_values()
        data = table[header_row + 1 :]
        columns = table[header_row]
        df = pd.DataFrame(data=data, columns=columns)

        # Filter columns and format
        cols_to_keep = list(col_renaming.keys() & df.columns)
        df = df[cols_to_keep].copy()
        df.rename(columns=col_renaming, inplace=True)
        df = df.map(lambda s: s.strip(), na_action='ignore')  # type: ignore
        df.replace({'TRUE': True, 'FALSE': False}, inplace=True)

        df.set_index(index_col, inplace=True)
        df.index.rename('', inplace=True)
        duplicated = df.index.duplicated()
        df = df.loc[~duplicated].copy()
        return df

    # def to_df(
    #     self, col_renaming: dict[str, str] = {}, lib_pattern: str = LIBRARY_ID_PATTERN
    # ) -> pd.DataFrame:
    #     """Get a spreadsheet from Google Drive and convert to pandas.DataFrame

    #     Parameters
    #     ----------
    #         :param worksheet_index: The index of the sheet you want to get from the spreadsheet, defaults to 0
    #         :type worksheet_index: `int`
    #         :param col_renaming: A mapping between the column names in the Google Sheet and the column names desired in the returned df. Only columns in this dict will be kept, defaults to {}
    #         :type col_renaming: `dict[str, str]`, optional
    #         :param col_types:  A mapping between the column names in the df and the type they should be converted to. Note that the keys in this dict should be the values of the col_renaming dict, defaults to {}
    #         :type col_types: `dict[str, type]`, optional
    #         :param **kwargs: Keyword arguments to be passed to gspread.WorkSheet.get_all_records.

    #     Returns
    #     -------
    #         :return: The requested Google Sheet as a `pandas.DataFrame`
    #         :rtype: pd.DataFrame
    #     """
    #     # Initialize list of dataframes and get all tables in
    #     # spreadsheet as rows and as columns
    #     dfs = []
    #     tables = (
    #         (
    #             worksheet.get_values(major_dimension='rows'),
    #             worksheet.get_values(major_dimension='columns'),
    #         )
    #         for worksheet in self.worksheets()
    #     )

    #     # Some worksheets have the same columns. Initialize a pd.Series
    #     # tracking nan counts so as to pick the ones with the least
    #     best_nan_counts = pd.Series({col: inf for col in col_renaming.values()})

    #     # Iterate over table-pairings
    #     for table_as_rows, table_as_columns in tables:
    #         cols_in_sheet, header_row_idx, header_row = set(), 0, []

    #         # Iterate over the rows of the table
    #         for i, row in enumerate(table_as_rows):
    #             # If this row shares elements with the desired columns
    #             # then it's the header row.
    #             cols_in_sheet = col_renaming.keys() & row
    #             if cols_in_sheet:
    #                 header_row_idx, header_row = i, row
    #                 break

    #         # If we've looped over all rows of the table and found
    #         # no rows that contain at least one of our desired keys,
    #         # move onto the next worksheet of this spreadsheet
    #         if not cols_in_sheet:
    #             continue

    #         # Get a list of the columns that contain values that match
    #         # the regex of a library ID
    #         library_col_idxs = [
    #             i
    #             for i, col in enumerate(table_as_columns)
    #             if any(match(pattern=lib_pattern, string=entry) for entry in col)
    #         ]

    #         # If it's empty, that means that this table doesn't have a
    #         # column tracking library ID, so its useless to us because
    #         # we can't align worksheets. # TODO: perhaps make this more
    #         # dynamic in case one wants to retrieve a spreadsheet that
    #         # has nothing to with libraries
    #         if not library_col_idxs:
    #             continue

    #         # Take the first column that matches the requirement above
    #         library_col_idx = library_col_idxs[0]

    #         # Construct DataFrame, assuming all data will be after
    #         # header row
    #         data = table_as_rows[header_row_idx + 1 :]
    #         df = pd.DataFrame(data=data, columns=header_row)

    #         # Format df
    #         df = df.map(lambda value: value.strip() if isinstance(value, str) else value)  # type: ignore
    #         df.replace({'TRUE': True, 'FALSE': False}, inplace=True)

    #         # The column name to index on will be the element in the
    #         # header row in the position defined as library_col_idx
    #         index_col_name = header_row[library_col_idx]
    #         df.set_index(index_col_name, inplace=True)
    #         df.index.rename(None, inplace=True)

    #         # Drop rows with duplicate indexes because the
    #         # tracking sheet has many empty rows
    #         duplicate_mask = df.index.duplicated()
    #         df = df.loc[~duplicate_mask]

    #         # Subset to the columns we care about and rename
    #         to_keep = list(cols_in_sheet - {index_col_name})
    #         df = df[to_keep]
    #         df.rename(columns=col_renaming, inplace=True)

    #         # Compare the nan counts, setting to_keep = True for
    #         # columns in this df that have less nans than the best so
    #         # far
    #         nan_counts = df.isna().sum()
    #         duplicate_cols = df.columns.intersection(best_nan_counts.index)
    #         to_keep = nan_counts[duplicate_cols] < best_nan_counts[duplicate_cols]

    #         # Get the actual column names to keep and subset df again
    #         cols_to_keep = to_keep[to_keep].index
    #         df = df[cols_to_keep]

    #         # Set the best nan counts and append df to dfs list
    #         best_nan_counts[cols_to_keep] = nan_counts[cols_to_keep]
    #         dfs.append(df)

    #     # Concatenate the dataframes and return
    #     return pd.concat(dfs, axis=1)


def get_project_params(
    df_row: pd.Series, metrics_dir_id: str, gclient: gs.Client, **kwargs
) -> pd.Series:
    """Use with pandas.DataFrame.apply to get tool version and reference path

    Parameters
    ----------
        :param df_row: The passed-in row of the pandas.DataFrame. It should contain the keys "project", "tool", "reference_dir", and "sample_name"
        :type df_row: `pd.Series`
        :param metrics_dir_id: The ID of the Google Drive folder containing delivered metrics
        :type metrics_dir_id: `str`

    Returns
    -------
        :return: A `dict` with keys 'tool_version' and 'reference_path'
        :rtype: `dict[str, str]`
    """
    from googleapiclient.discovery import build

    from .samplesheet import genomes_from_user, get_latest_version

    # Get credentials, project, tool, and reference dirs
    creds = gclient.auth
    sample_name, project, tool, reference_dirs = (
        df_row[col] for col in ('sample_name', 'project', 'tool', 'reference_dirs')
    )

    # Build service
    service = build(serviceName='drive', version='v3', credentials=creds)

    # Get all files in Google Drive folder matching criteria, sort
    # by modified time, and get their IDs
    result = (
        service.files()
        .list(
            corpora='user',
            q=f"fullText contains '{project}' and fullText contains '{tool}' and mimeType='application/vnd.google-apps.spreadsheet' and '{metrics_dir_id}' in parents",
            fields='files(id, modifiedTime, mimeType, parents)',
        )
        .execute()
    )
    metrics_files = result['files']
    metrics_files.sort(key=lambda file: file['modifiedTime'])
    spreadsheet_ids = [file['id'] for file in metrics_files]

    # Iterate over all spreadsheets
    for id in spreadsheet_ids:
        # Load all worksheets of file into dataframes
        metricssheet = GSheet(client=gclient, properties={'id': id})
        metrics_dfs = [
            metricssheet.to_df(sheet_idx=idx, **kwargs)
            for idx, _ in enumerate(metricssheet.worksheets())
        ]

        # Join them
        metrics_df = pd.concat(metrics_dfs, axis=1)
        # metrics_dfs[0].join(other=metrics_dfs[1:], on='libraries', how='outer', rsuffix='1')  # type: ignore

        # Filter metrics_df to contain just those projects matching this
        # project and tool
        project_df = metrics_df[
            (metrics_df['project'] == project) & (metrics_df['tool'] == tool)
        ].copy()

        if project_df.shape[0] < 1:
            continue

        # Construct the reference path, then convert it to a str
        # representation
        project_df['reference_path'] = project_df['reference'].map(
            lambda genome: [str(ref_dir / genome) for ref_dir in reference_dirs]
        )

        # Since all rows should be the same (as they belong to the same
        # project and have the same tool), return the first row
        return project_df[['tool_version', 'reference_path']].iloc[0]

    # Either there were no spreadsheets matching the criteria or there
    # are no instances of the project that share this tool. Just get
    # the latest tool version and reference genome from the user
    params = pd.Series()
    params['tool_version'] = get_latest_version(tool)

    message = f'\nThere are no spreadsheets in the Google Drive folder at https://drive.google.com/drive/folders/{metrics_dir_id} containing the SCBL Project [bold orange1]{project}[/] and the tool [bold orange1]{tool}[/].'
    params['reference_path'] = genomes_from_user(
        message=message,
        reference_dirs=reference_dirs,
        sample_name=sample_name,
    )

    return params
