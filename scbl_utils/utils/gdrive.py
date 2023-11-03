from pathlib import Path

import gspread as gs
import pandas as pd
from rich import print as rprint


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
        self,
        worksheet_index: int = 0,
        col_renaming: dict[str, str] = {},
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
        records = worksheet.get_all_records(
            expected_headers=col_renaming.keys(), **kwargs
        )
        df = pd.DataFrame.from_records(records)

        # Subset to desired columns, strip whitespace, convert "TRUE"
        # and "FALSE" to bools, rename columns, and convert n_cells to
        # numeric
        df = df[col_renaming.keys()]
        df = df.map(lambda value: value.strip() if isinstance(value, str) else value)  # type: ignore
        df.rename(columns=col_renaming, inplace=True)
        df.replace({'TRUE': True, 'FALSE': False}, inplace=True)

        return df


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
    from rich.prompt import Prompt

    from .samplesheet import get_latest_version

    # Get credentials, project name, and tool
    creds = gclient.auth
    sample_name = df_row['sample_name']
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
    reference_dirs = df_row['reference_dirs']

    # If Google Drive query returned nothing, use latest tool version
    # and get the proper reference path from the user
    if not result['files']:
        params = pd.Series()

        params['tool_version'] = get_latest_version(tool)
        rprint(
            f'\nIt appears that sample [bold orange1]{sample_name}[/] is associated with a new project, as its project ID ([bold orange1]{project}[/]) was not found in any of the spreadsheets in https://drive.google.com/drive/folders/{metrics_dir_id}. Please select a reference genome in each of the reference directories below:'
        )

        params['reference_path'] = []
        for ref_dir in reference_dirs:
            genome_choices = [path.name for path in ref_dir.iterdir()]
            genome_choices.sort()

            genome = Prompt.ask(
                f'[bold green]{ref_dir.absolute()} ->[/]', choices=genome_choices
            )
            full_ref_path = str((ref_dir / genome).absolute())
            params['reference_path'].append(full_ref_path)

        return params

    # Get the most recently modified delivered metrics spreadsheet
    # and convert to pandas.DataFrame
    most_recent = max(result['files'], key=lambda f: f['modifiedTime'])
    spreadsheet_id = most_recent['id']
    metricssheet = GSheet(client=gclient, properties={'id': spreadsheet_id})
    metrics_df = metricssheet.to_df(**kwargs)

    # Filter metrics_df to contain just those projects matching this
    # project and tool
    project_df = metrics_df[
        (metrics_df['project'] == project) & (metrics_df['tool'] == tool)
    ].copy()

    # Construct the reference path, then convert it to a str
    # representation
    project_df['reference_path'] = project_df['reference'].map(
        lambda genome: [str(ref_dir / genome) for ref_dir in reference_dirs]
    )

    # Since all rows should be the same (as they belong to the same
    # project and have the same tool), return the first row
    return project_df[['tool_version', 'reference_path']].iloc[0]

    # TODO: is there a case where a two runs of a project use the same
    # tool but different processing references? like, two different
    # species within a project?
