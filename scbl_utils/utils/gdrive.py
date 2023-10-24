import gspread as gs
import pandas as pd


def login(*args, **kwargs) -> gs.Client:
    """Log into Google Drive and return gspread.Client.

    :raises RuntimeError: If login impossible, raise error
    :return: The logged in client
    :rtype: gs.Client
    """
    from .defaults import DOCUMENTATION

    try:
        return gs.service_account(*args, **kwargs)
    except Exception as e:
        raise RuntimeError(
            f'Could not log into Google Drive. See {DOCUMENTATION} for instructions on authentication. {e}'
        )


class GSheet(gs.Spreadsheet):
    """Inherits from gspread.Spreadsheet, adding a to_df method. Constructor requires same args as gspread.SpreadSheet"""

    def to_df(
        self,
        worksheet_index: int = 0,
        col_renaming: dict = {},
        col_types: dict = {},
        **kwargs,
    ) -> pd.DataFrame:
        """Get a spreadsheet from Google Drive and convert to pandas.DataFrame

        Parameters
        ----------
            :param worksheet_index: The index of the sheet you want to get from the spreadsheet, defaults to 0
            :type worksheet_index: `int`
            :param col_renaming: A mapping between the column names in the Google Sheet and the column names desired in the returned df. Only columns in this dict will be kept, defaults to {}
            :type col_renaming: `dict[str, str]`, optional
            :param col_types:  A mapping between the column names in the df and the type they should be converted to. Note that the keys in this dict should be the values of the col_renaming dict, defaults to {}
            :type col_types: `dict[str, type]`, optional
            :param **kwargs: Keyword arguments to be passed to gspread.WorkSheet.get_all_records.

        Returns
        -------
            :return: The requested Google Sheet as a `pandas.DataFrame`
            :rtype: pd.DataFrame
        """
        # Get worksheet and convert to pd.DataFrame
        worksheet = self.get_worksheet(worksheet_index)
        records = worksheet.get_all_records(expected_headers=col_renaming, **kwargs)
        df = pd.DataFrame.from_records(records)

        # Subset to desired columns, rename, strip whitespace, convert
        # "TRUE" and "FALSE" to bools
        df = df[col_renaming.keys()]
        df.rename(columns=col_renaming, inplace=True)
        df.replace({'TRUE': True, 'FALSE': False}, inplace=True)

        # Cast df columns to desired types
        for col, dtype in col_types.items():
            df[col] = df[col].astype(dtype, errors='ignore')

        return df


def get_project_params(
    df_row: pd.Series, metrics_dir_id: str, gclient: gs.Client, **kwargs
) -> pd.Series:
    """Use with pandas.DataFrame.agg to get tool version and reference path

    Parameters
    ----------
        :param df_row: The passed-in row of the pandas.DataFrame. Its name should be a sample ID and it should contain the keys "project", "tool", and "reference_dir"
        :type df_row: `pd.Series`
        :param metrics_dir_id: The ID of the Google Drive folder containing delivered metrics
        :type metrics_dir_id: `str`

    Returns
    -------
        :return: A `dict` with keys 'tool_version' and 'reference_path'
        :rtype: `dict[str, str]`
    """
    from googleapiclient.discovery import build
    from rich.prompt import Prompt

    from .samplesheet import get_latest_tool_version

    # Get credentials, project name, and tool
    creds = gclient.auth
    project = df_row['project']
    tool = df_row['tool']

    # Build service
    service = build(serviceName='drive', version='v3', credentials=creds)

    # Get all files in Google Drive folder matching criteria
    result = (
        service.files()
        .list(
            corpora='user',
            q=f"fullText contains '{project}' and fullText contains '{tool}' and mimeType='application/vnd.google-apps.spreadsheet' and '{metrics_dir_id}' in parents",
            fields='files(id, modifiedTime, mimeType, parents)',
        )
        .execute()
    )

    # Get reference directory
    reference_dir = df_row['reference_dir']

    # If Google Drive query returned nothing, use latest tool version
    # and get the proper reference path from the user
    if not result['files']:
        params = pd.Series()

        params['tool_version'] = get_latest_tool_version(df_row['tool'])
        params['reference_path'] = Prompt.ask(f'It appears that sample {df_row.name} is associated with a new project, as its project ID ({project}) was not found in any of the spreadsheets in https://drive.google.com/drive/folders/{metrics_dir_id}. Please enter the reference genome in {reference_dir.absolute()} you want to use', choices=[path.name for path in reference_dir.iterdir()])  # type: ignore

        return params

    # Get the most recently modified delivered metrics spreadsheet
    # and convert to pandas.DataFrame
    most_recent = max(result['files'], key=lambda f: f['modifiedTime'])
    spreadsheet_id = most_recent['id']
    metricssheet = GSheet(client=gclient, properties={'id': spreadsheet_id})
    metrics_df = metricssheet.to_df(**kwargs)

    # Filter metrics_df to contain just those projects matching this
    # project
    project_df = metrics_df[
        (metrics_df['project'] == project) & (metrics_df['tool'] == tool)
    ].copy()

    # Construct the reference path, then convert it to a str
    # representation
    project_df['reference_path'] = reference_dir / project_df['reference']
    project_df['reference_path'] = project_df['reference_path'].apply(
        lambda path: str(path.absolute())
    )

    # Since all rows should be the same (as they belong to the same
    # project), return the first row
    return project_df.loc[0, ['tool_version', 'reference_path']]
