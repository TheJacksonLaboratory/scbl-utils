from pathlib import Path

import gspread as gs
import pandas as pd
from numpy import nan
from rich import print as rprint
from typer import Abort

from .defaults import (DOCUMENTATION, SPECIES_TO_GENOME_PATTERN,
                       TRACKING_DF_INDEX_COL)


def login(*args, **kwargs) -> gs.Client:  # type: ignore
    """Log into Google Drive and return gspread.Client.

    :raises RuntimeError: If login impossible, raise error
    :return: The logged in client
    :rtype: gs.Client
    """
    try:
        return gs.service_account(*args, **kwargs)
    except Exception as e:
        rprint(
            f'Could not log into Google Drive. See {DOCUMENTATION} for instructions on authentication. {e}'
        )
        raise Abort()


def load_specs(config_files: dict[str, Path]) -> tuple[dict, dict]:
    """Load specifications from google-drive config directory

    :param config_files: A dict mapping the name of a config file to its path
    :type config_files: dict[str, Path]
    :return: The tracking sheet specification and the metrics sheet specification, as a tuple in that order
    :rtype: tuple[dict, dict]
    """
    from jsonschema import ValidationError
    from jsonschema import validate as validate_yml
    from yaml import Loader, load

    from .defaults import SPEC_SCHEMA
    from .validate import metrics_spec as validate_metrics_spec
    from .validate import tracking_spec as validate_tracking_spec

    # Load in the two specification files that instruct script how to
    # get information from Google Drive
    specs = {
        filename: load(path.read_text(), Loader)
        for filename, path in config_files.items()
        if 'spec.yml' in filename
    }

    # Validate the yml's structure
    for filename, spec in specs.items():
        try:
            validate_yml(instance=spec, schema=SPEC_SCHEMA.get(filename, {}))
        except ValidationError as e:
            rprint(
                f'[green]{filename}[/] is incorrectly formatted. See {DOCUMENTATION} for more information.\n{e}'
            )
            raise Abort()

    # Validate each's contents as well
    validate_tracking_spec(specs['trackingsheet-spec.yml'])
    validate_metrics_spec(specs['metricssheet-spec.yml'])

    return specs['trackingsheet-spec.yml'], specs['metricssheet-spec.yml']


class GSheet(gs.Spreadsheet):
    """Inherits from gspread.Spreadsheet, adding a to_df method. Constructor requires same args as gspread.SpreadSheet"""

    def to_df(
        self,
        sheet_id: str,
        col_renaming: dict[str, str] = {},
        header_row: int = 0,
        index_col: str = TRACKING_DF_INDEX_COL,
        to_join: bool = True,
    ) -> pd.DataFrame:
        # Get table and assign data and header row, then convert to df
        table = self.get_worksheet_by_id(sheet_id).get_values(combine_merged_cells=True)
        data = table[header_row + 1 :]
        columns = table[header_row]
        df = pd.DataFrame(data=data, columns=columns)

        # Filter columns and format
        cols_to_keep = list(col_renaming.keys() & df.columns)
        df = df[cols_to_keep].copy()
        df.rename(columns=col_renaming, inplace=True)
        df = df.map(lambda s: s.strip(), na_action='ignore')  # type: ignore
        df.replace({'TRUE': True, 'FALSE': False, '': nan, '-': nan}, inplace=True)

        # Set index and rename
        df.set_index(index_col, inplace=True)
        df.index.rename('', inplace=True)

        # Duplicate indices will throw an error when joining
        if to_join:
            duplicated = df.index.duplicated()
            df = df.loc[~duplicated].copy()

        return df


def get_project_params(
    df_row: pd.Series,
    metrics_dir_id: str,
    gclient: gs.Client,
    species_to_genome_pattern: dict[str, str] = SPECIES_TO_GENOME_PATTERN,
    **kwargs,
) -> pd.Series:
    """Use with pandas.DataFrame.apply to get tool version and reference path

    Parameters
    ----------
        :param df_row: The passed-in row of the pandas.DataFrame. It should contain the keys "project", "tool", "reference_dir", and "sample_name"
        :type df_row: `pd.Series`
        :param metrics_dir_id: The ID of the Google Drive folder containing delivered metrics
        :type metrics_dir_id: `str`
        :param species_to_genome_pattern: A mapping of species -> regex for genomes of that species
        :type species_to_genome_pattern: `dict`
        :param **kwargs: key-word arguments passed into `GSheet.to_df`

    Returns
    -------
        :return: A `dict` with keys 'tool_version' and 'reference_path'
        :rtype: `dict[str, str]`
    """
    from googleapiclient.discovery import build

    from .samplesheet import genomes_from_user, get_latest_version

    # Get credentials, project, tool, and reference dirs
    creds = gclient.auth
    sample_name, project, tool, reference_dirs, species = (
        df_row[col]
        for col in ('sample_name', 'project', 'tool', 'reference_dirs', 'species')
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

    genome_pattern = species_to_genome_pattern.get(species, r'.*')
    # Iterate over all spreadsheets
    for id in spreadsheet_ids:
        try:
            # Load all worksheets of file into dataframes
            metricssheet = GSheet(client=gclient, properties={'id': id})
            metrics_dfs = [
                metricssheet.to_df(sheet_id=sheet.id, **kwargs)
                for sheet in metricssheet.worksheets()
            ]
        except gs.exceptions.APIError:
            rprint(
                f'In trying to assign the [green]tool_version[/] and [green]reference_path[/] for [bold orange1]{sample_name}[/], the [red]API request-rate limit was exceeded[/]. Defaulting to latest [green]tool_version[/] and asking you to input [green]reference_path[/].'
            )
            break

        # The below chunk of code will figure out whether each tab in
        # in the metrics spreadsheet represents the same information
        # about different libraries, or different information about
        # the same libraries. # TODO: put in to a function

        # First, figure out whether the columns are the same between
        # sheets
        cols = [col for df in metrics_dfs for col in df.columns]
        set_cols = set(cols)

        # If they are, then it's the same info but for different
        # libraries. If not, then it's different info for same
        # libraries.
        if len(cols) != len(set_cols):
            metrics_df = pd.concat(metrics_dfs, axis=0, join='outer')
        else:
            metrics_df = pd.concat(metrics_dfs, axis=1, join='outer')

        # metrics_dfs[0].join(other=metrics_dfs[1:], on='libraries', how='outer', rsuffix='1')  # type: ignore

        # Filter metrics_df to contain just those projects matching this
        # project and tool
        project_df = metrics_df[
            (metrics_df['project'] == project)
            & (metrics_df['tool'] == tool)
            & (metrics_df['reference'].str.match(genome_pattern, case=False))
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
