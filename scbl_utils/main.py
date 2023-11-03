from pathlib import Path
from typing import Annotated

import typer

from .utils import validate
from .utils.defaults import CONFIG_DIR, DOCUMENTATION, REF_PARENT_DIR

app = typer.Typer()


@app.callback(no_args_is_help=True)
def callback(config_dir: Path = CONFIG_DIR) -> None:
    """
    Command-line utilities that facilitate data processing in the
    Single Cell Biology Lab at the Jackson Laboratory.
    """
    global CONFIG_DIR
    CONFIG_DIR = config_dir
    _ = validate.direc(config_dir)


@app.command(no_args_is_help=True)
def samplesheet_from_gdrive(
    fastqdirs: Annotated[
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
    ref_parent_dir: Annotated[
        Path,
        typer.Option(
            '--reference-parent-dir',
            '-r',
            help=f'The parent directory where all reference genomes are stored, organized into directories that correspond to fastq processing tools. See {DOCUMENTATION} for more information.',
        ),
    ] = REF_PARENT_DIR,
) -> None:
    """
    Pull data from Google Drive and generate a yml samplesheet to be
    used as input for the nf-tenx pipeline.
    """
    from pandas import to_numeric
    from yaml import Dumper, add_representer, dump

    from .utils import gdrive
    from .utils.defaults import GDRIVE_CONFIG_FILES, SAMPLESHEET_KEY_TO_TYPE, SCOPES
    from .utils.samplesheet import (
        map_libs_to_fastqdirs,
        program_from_lib_types,
        sanitize_sample_name,
        sequence_representer,
    )

    # Create and validate Google Drive config dir, returning paths
    gdrive_config_dir = CONFIG_DIR / 'google-drive'
    config_files = validate.direc(gdrive_config_dir, GDRIVE_CONFIG_FILES)

    # Construct mapping between each library and its directory and
    # and validate at the same time
    lib_to_fastqdir = map_libs_to_fastqdirs(fastqdirs)

    # Load in the two specification files that instruct script how to
    # get information from Google Drive
    tracking_spec, metrics_spec = gdrive.load_specs(config_files)

    # Login and get trackingsheet
    gclient = gdrive.login(scopes=SCOPES, filename=config_files['service-account.json'])
    trackingsheet = gdrive.GSheet(
        client=gclient, properties={'id': tracking_spec['id']}
    )

    # Convert GSheet to pd.DataFrame, filter
    tracking_df = trackingsheet.to_df(
        col_renaming=tracking_spec['columns'], head=tracking_spec['header_row']
    )
    samplesheet_df = tracking_df[tracking_df['libraries'].isin(lib_to_fastqdir)].copy()
    samplesheet_df['n_cells'] = to_numeric(samplesheet_df['n_cells'], errors='coerce')

    # Fill samplesheet with available information
    samplesheet_df['fastq_paths'] = samplesheet_df['libraries'].map(lib_to_fastqdir)
    samplesheet_df['library_types'] = samplesheet_df['10x_platform'].map(
        tracking_spec['platform_to_lib_type']
    )

    # Group by sample_name and fill with library type specific information
    grouped_samplesheet_df = samplesheet_df.groupby(
        'sample_name', as_index=False
    ).apply(program_from_lib_types, ref_parent_dir=ref_parent_dir.absolute())

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

    # Sanitize sample names
    grouped_samplesheet_df['sample_name'] = grouped_samplesheet_df['sample_name'].map(
        sanitize_sample_name
    )

    # Drop unnecessary columns
    to_keep = [
        col for col in SAMPLESHEET_KEY_TO_TYPE if col in grouped_samplesheet_df.columns
    ]
    grouped_samplesheet_df = grouped_samplesheet_df[to_keep]

    # Sort and group for prettier yml output
    grouped_samplesheet_df.sort_values(['library_types', 'sample_name'], inplace=True)

    # Actually write output
    with output_path.open('w') as f:
        # Add custom representer for Collections
        for sequence_type in (list, tuple):
            add_representer(data_type=sequence_type, representer=sequence_representer)

        delimiter = '#' * 80
        for _, group in grouped_samplesheet_df.groupby('library_types'):
            # Convert to records and filter out nans
            records = group.to_dict(orient='records')
            records = [
                {key: value for key, value in rec.items() if value == value}
                for rec in records
            ]

            # Dump and write delimiter between groups
            dump(
                data=records,
                stream=f,
                Dumper=Dumper,
                default_flow_style=False,
                sort_keys=False,
            )
            f.write(f'\n{delimiter}\n\n')
