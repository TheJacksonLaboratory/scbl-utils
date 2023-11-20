from collections.abc import Collection
from pathlib import Path

from rich import print as rprint
from typer import Abort

from .defaults import DOCUMENTATION, REQUIRED_METRICSSHEET_SPEC_KEYS, REQUIRED_TRACKINGSHEET_SPEC_COLUMNS, TRACKING_DF_INDEX_COL


def direc(direc: Path, required_files: Collection[Path] = []) -> dict[str, Path]:
    """Checks that `direc` has required files and returns them, creating `direc` in the process

    :param direc: Config dir to check
    :type direc: Path
    :param required_files: Filenames required to exist in direc
    :type required_files: list[Path]
    :raises FileNotFoundError: Raise error if required files not found in direc
    :return: A dict mapping the filename to its absolute path
    :rtype: dict[str, Path]
    """
    # Create directory if it doesn't exist
    direc.mkdir(exist_ok=True, parents=True)

    # Generate absolute paths for required files, find missing ones
    required_paths = {path.name: (direc / path).absolute() for path in required_files}
    missing_files = '\n'.join(
        filename for filename, path in required_paths.items() if not path.exists()
    )

    if missing_files:
        rprint(
            f'Please place the following files in {direc.absolute()}. See {DOCUMENTATION} for more details.\n{missing_files}'
        )
        raise Abort()

    return required_paths


def tracking_spec(spec: dict[str, dict[str, str] | str], required_columns : set = REQUIRED_TRACKINGSHEET_SPEC_COLUMNS, index_col: str = TRACKING_DF_INDEX_COL):
    sheets = spec['sheets']
    spec_columns = {col for sheet_dict in sheets.values() for col in sheet_dict['columns'].values()}
    missing_columns = '\n'.join(required_columns - spec_columns)
    if missing_columns:
        id = spec['id']
        rprint(f'The following columns are missing from [green]trackingsheet-spec.yml[/]:', f'[bright_cyan{missing_columns}[/]', f'Please map the columns in https://docs.google.com/spreadsheets/d/{id} to these missing columns and place mapping in [bright_cyan]trackingsheet-spec.yml[/].')
        raise Abort()
    
    if any(index_col not in sheet_dict['columns'].values() for sheet_dict in sheets.values()):
        rprint(f'Every sheet in [bright_cyan]trackingsheet-spec.yml[/] must have a column mapping to [green]{index_col}[/].')
        raise Abort()
    
    return


def metrics_spec(spec: dict[str, str | dict[str, str] | int], required_columns: set = REQUIRED_METRICSSHEET_SPEC_KEYS):
    spec_columns = spec['columns']
    missing_columns = '\n'.join(required_columns - spec_columns.values())
    
    if missing_columns:
        rprint(f'The following columns are missing from [green]metricssheet-spec.yml[/]:', f'[bright_cyan{missing_columns}[/]', f'Please map the columns in a given delivery metrics sheet to these missing columns and place mapping in [bright_cyan]metricssheet-spec.yml[/].')
        raise Abort()
    return