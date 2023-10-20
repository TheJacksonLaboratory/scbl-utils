from pathlib import Path
from typing import Annotated

import typer

from .utils.defaults import CONFIG_DIR

app = typer.Typer()


@app.callback(no_args_is_help=True)
def callback(config_dir: Path = CONFIG_DIR) -> None:
    """
    Command-line utilities that facilitate data processing in the
    Single Cell Biology Lab at the Jackson Laboratory.
    """
    global CONFIG_DIR
    CONFIG_DIR = config_dir
    CONFIG_DIR.mkdir(exist_ok=True, parents=True)


@app.command(no_args_is_help=True)
def samplesheet_from_gdrive(
    fastq_dirs: Annotated[
        list[Path],
        typer.Argument(
            help='Directories containing fastq files for which to generate samplesheet.'
        ),
    ],
    output_path: Annotated[
        Path,
        typer.Option(
            '--outsheet', '-o', help='File path to save the resulting samplesheet.'
        ),
    ] = Path('samplesheet.yml'),
) -> None:
    """
    Pull data from Google Drive to generate a yml samplesheet to be
    used as input for the TheJacksonLaboratory/nf-tenx pipeline.
    """
    from itertools import groupby
    from re import sub
    from string import ascii_letters, digits

    from yaml import Dumper, Loader, dump, load

    from .utils import gdrive, validate
    from .utils.defaults import GDRIVE_CONFIG_FILES, SAMPLESHEET_KEY_TO_TYPE, SCOPES
    from .utils.samplesheet import get_program_from_lib_types, map_libs_to_fastqdirs

    # Create and validate Google Drive config dir
    gdrive_config_dir = CONFIG_DIR / 'google-drive'
    gdrive_config_dir.mkdir(exist_ok=True, parents=True)
    config_files = validate.config_dir(gdrive_config_dir, GDRIVE_CONFIG_FILES)

    # Construct mapping between each library and its directory and
    # and validate at the same time
    lib_to_fastqdir = map_libs_to_fastqdirs(fastq_dirs)

    # Load in the two specification files that instruct script how to
    # get information from Google Drive
    specs = {
        filename: load(path.read_text(), Loader)
        for filename, path in config_files.items()
        if 'spec.yml' in filename
    }
    tracking_spec, metrics_spec = (
        specs['trackingsheet_spec.yml'],
        specs['metricssheet_spec.yml'],
    )

    # Login and get trackingsheet
    gclient = gdrive.login(scopes=SCOPES, filename=config_files['service-account.json'])
    trackingsheet = gdrive.GSheet(
        client=gclient, properties={'id': tracking_spec['id']}
    )

    # Get mappings between column names in the spreadsheet and their
    # new names and data types
    col_renaming = {
        col['old_name']: col['new_name'] for col in tracking_spec['columns']
    }
    col_types = {col['new_name']: col['type'] for col in tracking_spec['columns']}

    # Convert GSheet to pd.DataFrame, filter (copying so as to not
    # edit a view)
    tracking_df = trackingsheet.to_df(
        col_renaming=col_renaming, col_types=col_types, head=tracking_spec['header_row']
    )
    samplesheet_df = tracking_df[tracking_df['libraries'].isin(lib_to_fastqdir)].copy()

    # Fill samplesheet with available information
    samplesheet_df['fastq_paths'] = samplesheet_df['libraries'].map(lib_to_fastqdir)
    samplesheet_df['library_types'] = samplesheet_df['10x_platform'].map(
        tracking_spec['platform_to_lib_type']
    )

    # Group by sample_name and fill with library type-specific information
    grouped_samplesheet_df = samplesheet_df.groupby('sample_name').apply(
        get_program_from_lib_types
    )

    # Also get tool version and reference path
    grouped_samplesheet_df[
        ['tool_version', 'reference_path']
    ] = grouped_samplesheet_df.apply(
        gdrive.get_project_params,
        axis=1,
        metrics_dir_id=metrics_spec['dir_id'],
        gclient=gclient,
        head=metrics_spec['header_row'],
        col_renaming=metrics_spec['columns'],
    )

    # Sanitize sample names (ascii letters, digits, and '-' allowed)
    blacklist = f'[^{ascii_letters + digits}]'
    grouped_samplesheet_df['sample_name'] = grouped_samplesheet_df['sample_name'].apply(
        lambda sample_name: sub(pattern=blacklist, repl='-', string=sample_name)
    )

    # Drop excess columns and sort for groupby operation later
    to_keep = grouped_samplesheet_df.columns.intersection(SAMPLESHEET_KEY_TO_TYPE)  # type: ignore
    grouped_samplesheet_df = grouped_samplesheet_df[to_keep]
    grouped_samplesheet_df.sort_values(
        by=['tool', 'command', 'sample_name'], inplace=True
    )
    samplesheet_records = grouped_samplesheet_df.to_dict(orient='records')

    # Write to yml, grouping by tool-command combo for easier visual
    # parsing
    with output_path.open('w') as f:
        for _, records in groupby(
            samplesheet_records, key=lambda record: (record['tool'], record['command'])
        ):
            dump(
                list(records),
                stream=f,
                Dumper=Dumper,
                sort_keys=False,
                default_flow_style=False,
            )
            f.write('###\n')
