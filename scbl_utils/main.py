from pathlib import Path
from typing import Annotated

import typer

from .utils import validate
from .utils.defaults import CONFIG_DIR, DOCUMENTATION

see_more_message = f'See {DOCUMENTATION} for more information.'
app = typer.Typer()


@app.callback(no_args_is_help=True)
def callback(
    config_dir: Annotated[
        Path,
        typer.Option(
            '--config-dir',
            '-c',
            help='Configuration directory containing files necessary for script to run.',
        ),
    ] = CONFIG_DIR
) -> None:
    """
    Command-line utilities that facilitate data processing in the
    Single Cell Biology Lab at the Jackson Laboratory.
    """
    global CONFIG_DIR
    CONFIG_DIR = config_dir
    _ = validate.direc(config_dir)


# TODO: clean up this function. or not because it's gonna be obsolete
# TODO: if dataframe.apply, just pass the whole dataframe.
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
    Pull data from Google Drive and generate a `yml` samplesheet to be
    used as input for the nf-tenx pipeline.
    """
    from pandas import concat

    from .utils import gdrive
    from .utils.defaults import (AGG_FUNCS, GDRIVE_CONFIG_FILES,
                                 LIB_TYPES_TO_PROGRAM, SCOPES)
    from .utils.samplesheet import (get_antibody_tags, get_design,
                                    get_visium_info, map_libs_to_fastqdirs,
                                    map_platform_to_probeset,
                                    samplesheet_from_df, sanitize_samplename)

    # Create and validate Google Drive config dir, returning paths
    gdrive_config_dir = CONFIG_DIR / 'google-drive'
    config_files = validate.direc(gdrive_config_dir, GDRIVE_CONFIG_FILES)

    # Construct mapping between each library and its directory and
    # and validate at the same time
    lib_to_fastqdir = map_libs_to_fastqdirs(fastqdirs)

    # Load in the two specification files that instruct script how to
    # get information from Google Drive
    tracking_spec, metrics_spec = gdrive.load_specs(config_files)
    sheets_spec = tracking_spec['sheets']

    # Login and get trackingsheet
    gclient = gdrive.login(scopes=SCOPES, filename=config_files['service-account.json'])
    trackingsheet = gdrive.GSheet(
        client=gclient, properties={'id': tracking_spec['id']}
    )

    # Convert GSheet to pd.DataFrame and concatenate
    tracking_dfs = [
        trackingsheet.to_df(
            sheet_id=sheet_id,
            col_renaming=sheet_dict['columns'],
            header_row=sheet_dict['header_row'],
            to_join=sheet_dict['join'],
        )
        for sheet_id, sheet_dict in sheets_spec.items()
        if sheet_dict['join']
    ]
    tracking_df = concat(tracking_dfs, axis=1)
    tracking_df['libraries'] = tracking_df.index

    # This is hardcoded because eventually google-drive will become
    # irrelevant. However TODO: make the below less hardcoded or
    # prettier in a function or something
    multiplexing_sheet_id, multiplexing_spec = ((id, spec) for id, spec in sheets_spec.items() if not spec['join'])
    multiplexing_df = trackingsheet.to_df(
        sheet_id=multiplexing_sheet_id,
        col_renaming=multiplexing_spec['columns'],
        header_row=multiplexing_spec['header_row'],
        to_join=multiplexing_spec['join'],
    )

    # Filter df and fill with available information
    # # TODO: after the filtration, everything can be wrapped into a
    # function called "fill" or something
    samplesheet_df = tracking_df[tracking_df['libraries'].isin(lib_to_fastqdir.keys())].copy()  # type: ignore
    samplesheet_df['design'] = samplesheet_df['libraries'].apply(
        get_design, multiplexing_df=multiplexing_df
    )
    for new_col, old_col, mapping in (
        ('fastq_paths', 'libraries', lib_to_fastqdir),
        ('library_types', '10x_platform', tracking_spec['platform_to_lib_type']),
    ):
        samplesheet_df[new_col] = samplesheet_df[old_col].map(mapping)

    # Group by sample_name and aggregate
    grouped_samplesheet_df = samplesheet_df.groupby('sample_name', as_index=False).agg(
        AGG_FUNCS
    )

    # Assign tool/command/ref_dirs combo based on lib_types. This is
    # weird because pandas is weird
    grouped_samplesheet_df[
        ['tool', 'command', 'reference_dirs']
    ] = grouped_samplesheet_df['library_types'].apply(
        lambda lib_combo: LIB_TYPES_TO_PROGRAM[lib_combo]
    )

    # Some gene expression libraries are actually multiplexed despite
    # not being marked as such. If the 'design' column is there, then
    # change it to cellranger multi. Eventually there should be a
    # better way of doing this TODO
    grouped_samplesheet_df['command'] = grouped_samplesheet_df[
        ['command', 'design']
    ].apply(lambda s: 'multi' if s['design'] else s['command'], axis=1)

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

    # Get antibody tags if antibody capture
    grouped_samplesheet_df['tags'] = grouped_samplesheet_df['library_types'].map(
        get_antibody_tags
    )

    # Get probe set if flex or visium
    grouped_samplesheet_df['probe_set'] = grouped_samplesheet_df[
        ['10x_platform', 'species']
    ].apply(map_platform_to_probeset, axis=1)

    # Get visium file paths
    grouped_samplesheet_df = grouped_samplesheet_df.apply(get_visium_info, axis=1)

    # Sanitize sample names
    grouped_samplesheet_df['sample_name'] = grouped_samplesheet_df['sample_name'].map(
        sanitize_samplename
    )

    # Generate yml output and write to file, converting ref_path to str
    # if desired
    yml_output = (
        samplesheet_from_df(grouped_samplesheet_df, cols_as_str=['reference_path'])
        if ref_path_as_str
        else samplesheet_from_df(grouped_samplesheet_df)
    )
    output_path.write_text(data=yml_output)


# @app.command(no_args_is_help=True)
# def metrics_from_