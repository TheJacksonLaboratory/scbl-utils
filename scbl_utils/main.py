from pathlib import Path
from typing import Annotated

import typer

from .utils import validate
from .utils.defaults import CONFIG_DIR, DOCUMENTATION, REF_PARENT_DIR

see_more_message = f'See {DOCUMENTATION} for more information.'
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
            help=f'The parent directory where all reference genomes are stored, organized into directories that correspond to fastq processing tools. {see_more_message}',
        ),
    ] = REF_PARENT_DIR,
    ref_path_as_str: Annotated[
        bool,
        typer.Option(
            '--reference-path-as-str',
            '-s',
            help=f'If possible, write the "reference_path" field of the outputted samplesheet as a string rather than a list of strings. This enables compatability with nf-tenx pipeline and wll be deprecated in the future. {see_more_message}',
        ),
    ] = False,
) -> None:
    """
    Pull data from Google Drive and generate a yml samplesheet to be
    used as input for the nf-tenx pipeline.
    """
    from pandas import to_numeric

    from .utils import gdrive
    from .utils.defaults import GDRIVE_CONFIG_FILES, SCOPES
    from .utils.samplesheet import (
        fill_other_cols,
        map_libs_to_fastqdirs,
        program_from_lib_types,
        samplesheet_from_df,
        sanitize_sample_name,
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

    # Convert GSheet to pd.DataFrame
    tracking_df = trackingsheet.to_df(col_renaming=tracking_spec['columns'])

    # Filter df and convert n_cells to numeric
    samplesheet_df = tracking_df.loc[lib_to_fastqdir.keys()].copy()  # type: ignore
    samplesheet_df['n_cells'] = to_numeric(
        samplesheet_df['n_cells'].str.replace(',', ''), errors='coerce'
    )

    # Fill samplesheet with available information
    samplesheet_df['fastq_paths'] = samplesheet_df.index.map(lib_to_fastqdir)
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
        col_renaming=metrics_spec['columns'],
    )

    # Fill other columns based on library types
    grouped_samplesheet_df = grouped_samplesheet_df.apply(fill_other_cols, axis=1)

    # Sanitize sample names
    grouped_samplesheet_df['sample_name'] = grouped_samplesheet_df['sample_name'].map(
        sanitize_sample_name
    )

    # Generate yml output and write to file, converting ref_path to str
    # if desired
    yml_output = (
        samplesheet_from_df(grouped_samplesheet_df, cols_as_str=['reference_path'])
        if ref_path_as_str
        else samplesheet_from_df(grouped_samplesheet_df)
    )
    output_path.write_text(data=yml_output)
