from collections.abc import Collection, Sequence
from pathlib import Path
from re import match, sub

import pandas as pd
from numpy import nan
from rich import print as rprint
from typer import Abort
from yaml import Dumper, SequenceNode, add_representer, dump

from .defaults import (ANTIBODY_LIB_TYPES, CONTAINER_REGISTRY, LIBRARY_GLOB_PATTERN,
                       PLATFORMS_TO_PROBESET, SAMPLENAME_BLACKLIST_PATTERN,
                       SAMPLESHEET_GROUP_KEY, SAMPLESHEET_KEYS,
                       SAMPLESHEET_SORT_KEYS, SEP_PATTERN, SIBLING_REPOSITORY,
                       VISIUM_DIR)


def map_libs_to_fastqdirs(
    fastqdirs: Collection[Path], glob_pattern: str = LIBRARY_GLOB_PATTERN
) -> dict[str, str]:
    """Go from a list of fastq dirs to a mapping of library ID to fastq dir

    :param fastq_dirs: list of fastq dirs
    :type fastq_dirs: list[Path]
    :param glob_pattern: pattern to glob for in each fastq dir, defaults to `LIBRARY_GLOB_PATTERN`
    :type glob_pattern: str, optional
    :return: A dict mapping each library ID to its fastq directory as an string of its absolute path
    :rtype: dict[str, str]
    """
    # Get a mapping between library names and the fastq directories
    # they're in. Assumes a specific format for filenames
    lib_to_fastqdir = {
        path.name.split('_')[0]: str(fastqdir.absolute())
        for fastqdir in fastqdirs
        for path in fastqdir.glob(glob_pattern)
    }

    # Get the directories that do not contain any files matching the
    # glob pattern. If there are any, raise error
    input_dirs = {str(fastqdir.absolute()) for fastqdir in fastqdirs}
    matching_dirs = set(lib_to_fastqdir.values())
    bad_dirs = '\n'.join(input_dirs - matching_dirs)
    if bad_dirs:
        rprint(
            f'The following directories did not contain any files that match the glob pattern [blue bold]{glob_pattern}[/]:',
            bad_dirs,
            sep='\n',
        )

    # Sort dict before returning
    sorted_libs = sorted(lib_to_fastqdir.keys())
    sorted_lib_to_fastqdir = {lib: lib_to_fastqdir[lib] for lib in sorted_libs}
    return sorted_lib_to_fastqdir


def get_latest_version(
    tool: str, registry_url: str = CONTAINER_REGISTRY
) -> (
    str
):
    match tool:
        case 'cellranger':
            return '7.1.0'
        case 'spaceranger':
            return '2.1.0'
        case 'cellranger-atac':
            return '2.1.0'
        case 'cellranger-arc':
            return '2.0.1'
        case _:
            raise NotImplementedError(f'{tool} not available')


def get_latest_version_old(
    tool: str, registry_url: str = CONTAINER_REGISTRY
) -> (
    str
):
    """Get latest version of a given tool

    :param tool: The name of the tool
    :type tool: `str`
    :param repository_link: Link to repository containing a table to read information from, defaults to `SIBLING_REPOSITORY`
    :type repository_link: `str`, optional
    :return: The latest version of the tool
    :rtype: `str`
    """
    # Read the table from the README, rename columns, and format tool
    df = pd.read_html(registry_url)[0]
    latest_definition_file: str = df.loc[df['Recipe'].str.match(rf'{tool}-\d\.\d\.\d'), 'Recipe'].max()
    tool = latest_definition_file.removesuffix('.def')
    tool_version = tool.split('-')[-1]
    
    return tool_version
    # return latest_definition_file.split('')

    # df.rename(columns={col: col.lower() for col in df.columns}, inplace=True)
    # df['tool'] = df['tool'].str.replace(' ', '-').str.lower()

    # # Get the index of each row that actually has a tool name because
    # # some rows are blank
    # tool_idxs = df.dropna(subset='tool').index

    # # Append the index of the last row, as well as 1 + that index. This
    # # is relevant for the next step
    # tool_idxs = tool_idxs.append(pd.Index([df.index[-1], df.index[-1] + 1]))

    # # Pair each tool index with the index of the next row containing a tool name
    # idx_pairings = zip(tool_idxs, tool_idxs[1:])

    # # Fill the table so that there are no blank rows
    # for first_row_with_tool, last_row_with_tool in idx_pairings:
    #     df.loc[range(first_row_with_tool, last_row_with_tool), 'tool'] = df.loc[first_row_with_tool, 'tool']  # type: ignore

    # # Group by tool, get latest version for each tool, and format
    # grouped = df.groupby('tool')
    # latest_versions = grouped['version'].max()

    # return latest_versions[tool]


def genomes_from_user(
    message: str, reference_dirs: Collection[Path], sample_name: str, libraries: Collection[str]
) -> list[str]:
    """Get the genome for a given sample from the user

    :param message: The message to display to the user
    :type message: `str`
    :param reference_dirs: The directories containing the reference genomes to search
    :type reference_dirs: `Collection[Path]`
    :param sample_name: The name of the sample
    :type sample_name: `str`
    :return: The list of genomes to use for this sample
    :rtype: `list[str]`
    """
    from rich.prompt import Prompt

    # Print a message explaining why the user is being prompted
    rprint(message, end='\n\n')

    # The reference dirs and libraries are both in order, zip them
    reference_paths = []
    for ref_dir in reference_dirs:
        # Get the genome choices and sort for prettier output
        genome_choices = [path.name for path in ref_dir.iterdir()]
        genome_choices.sort()

        # Ask the user which genome they want and construct the full
        # ref path, appending to output
        genome = Prompt.ask(
            f'Choose a genome in [bold green]{ref_dir.absolute()}[/] for [bold orange1]{sample_name} {libraries}[/] ->',
            choices=genome_choices,
        )
        full_ref_path = str((ref_dir / genome).absolute())
        reference_paths.append(full_ref_path)

    return reference_paths


def get_antibody_tags(
    library_types: Collection[str], antibody_lib_types: set[str] = ANTIBODY_LIB_TYPES
) -> tuple[str, ...] | float:
    """Gets all TotalSeq™-B tags

    :param library_types: The library types associated with a given sample
    :type library_types: `Collection[str]`
    :param antibody_lib_types: The library types that should have antibody tags associated with them, defaults to `ANTIBODY_LIB_TYPES`
    :type antibody_lib_types: `set[str]`, optional
    :return: All TotalSeq™-B tags or `np.nan` if the assay does not require it
    :rtype: tuple[str, ...] | float
    """
    if not antibody_lib_types & set(library_types):
        return nan

    tags_df = pd.read_csv(
        'https://raw.githubusercontent.com/TheJacksonLaboratory/nf-tenx/main/assets/totalseq-b_universal.csv'
    )

    return tuple(tags_df['tag_id'])


def map_platform_to_probeset(
    df_row: pd.Series,
    platform_to_probset: dict[str, dict[str, str]] = PLATFORMS_TO_PROBESET,
) -> str | float:
    """If a given platform should have a probe-set, get it

    :param df_row: The row corresponding to the sample
    :type df_row: `pd.Series`
    :param platform_to_probset: A mapping of platform -> probe set, defaults to `PLATFORMS_TO_PROBESET`
    :type platform_to_probset: `dict[str, dict[str, str]]`, optional
    :return: The path of the probe set, relative to `SIBLING_REPOSITORY`/assets or `np.nan` if the assay is not one that requires a probe set
    :rtype: `str` | `np.nan`
    """
    platforms, species = (df_row[col] for col in ('10x_platform', 'species'))

    for platform in platforms:
        probeset_dict = platform_to_probset.get(platform)
        if not probeset_dict:
            continue
        probe_set = probeset_dict.get(species)
        if probe_set:
            return probe_set

    return nan


def get_visium_info(
    df_row: pd.Series,
    visium_dir: Path = VISIUM_DIR,
) -> pd.Series:
    """Visium samples require extra files, so get them if necessary

    :param df_row: The row corresponding to the sample
    :type df_row: `pd.Series`
    :param visium_dir: The directory in which to look for these files, defaults to `VISIUM_DIR`
    :type visium_dir: `Path`, optional
    :return: The same row, updated with the file paths necesary for Visium runs.
    :rtype: `pd.Series`
    """
    library = df_row['libraries'][0]
    slide, area, lib_types = (df_row[col] for col in ('slide', 'area', 'library_types'))

    new_data = df_row.copy()
    for lib in (library.upper(), library.lower()):
        shared_patterns = {
            'roi_json': rf'{lib}\.json',
            'image': rf'{lib}\.tiff',
            'manual_alignment': rf'{lib}[_-]{slide}[_-]{area}\.json',
        }
        cytassist_pattern = {'cyta_image': rf'CAV.*{lib}.*\.tif'}
        lib_type_to_patterns = {
            ('CytAssist Gene Expression',): shared_patterns | cytassist_pattern,
            ('Spatial Gene Expression',): shared_patterns,
        }
        patterns = lib_type_to_patterns.get(lib_types)
        if not patterns:
            return df_row

        paths_with_lib = list(visium_dir.rglob(f'*{lib}*'))
        for key, pattern in patterns.items():
            for path in paths_with_lib:
                if match(pattern=pattern, string=path.name):
                    new_data[key] = str(path.absolute())
                    break
    return new_data


def sanitize_samplename(
    sample_name: str, sample_name_blacklist_pattern: str = SAMPLENAME_BLACKLIST_PATTERN
) -> str:
    """Format sample names correctly, removing illegal characters and replacing spaces

    :param sample_name: The sample name to sanitize
    :type sample_name: `str`
    :param sample_name_blacklist_pattern: A blacklist regex of unallowed characters, defaults to `SAMPLENAME_BLACKLIST_PATTERN`
    :type sample_name_blacklist_pattern: `str`, optional
    :return: The sanitized sample name
    :rtype: `str`
    """
    legal = sub(pattern=sample_name_blacklist_pattern, repl='', string=sample_name)
    sep_replaced = sub(pattern=SEP_PATTERN, repl='-', string=legal)
    return sep_replaced


def sequence_representer(dumper: Dumper, data: list | tuple) -> SequenceNode:
    item = data[0]
    if isinstance(item, Collection) and not isinstance(item, str):
        return dumper.represent_list(data)
    else:
        return dumper.represent_sequence(
            tag='tag:yaml.org,2002:seq', sequence=data, flow_style=True
        )


def get_design(
    lib: str, multiplexing_df: pd.DataFrame
) -> dict[str, dict[str, str]] | float:
    """Gets the multiplexing design for a sample if applicable

    :param lib: The library ID in question
    :type lib: `str`
    :param multiplexing_df: A table representing the multiplexing tab of a sample tracking sheet
    :type multiplexing_df: `pd.DataFrame`
    :return: The multiplexing 'design'
    :rtype: dict[str, dict[str, str]] | float
    """
    if lib not in multiplexing_df.index:
        return nan

    lib_multiplexing_info = multiplexing_df.loc[lib].copy()
    if isinstance(lib_multiplexing_info, pd.DataFrame):
        design = {
            tag: {'name': name, 'description': description}
            for tag, name, description in lib_multiplexing_info[
                ['tag_id', 'sub_sample_name', 'description']
            ].itertuples(index=False)
        }

    elif isinstance(lib_multiplexing_info, pd.Series):
        design = {
            lib_multiplexing_info['tag_id']: {
                'name': lib_multiplexing_info['sub_sample_name'],
                'description': lib_multiplexing_info['description'],
            }
        }

    else:
        design = nan

    return design


def is_valid(value: Collection | str | int | bool | None | float) -> bool:
    """Determine whether a value should be printed to samplesheet

    :param value: The value to be printed
    :type value: `Collection | str | int | bool | None | float`
    :return: Whether the value should be printed or not
    :rtype: bool
    """
    if isinstance(value, Collection) and len(value) == 0:
        return False
    if value is None:
        return False
    if value != value:
        return False
    return True


def samplesheet_from_df(
    df: pd.DataFrame,
    output_cols: Sequence[str] = SAMPLESHEET_KEYS,
    cols_as_str: Collection[str] = [],
    sortby: list[str] | str = SAMPLESHEET_SORT_KEYS,
    groupby: list[str] | str = SAMPLESHEET_GROUP_KEY,
    delimiter: str = '#' * 80,
) -> str:
    """Convert a `pandas.DataFrame` to a `yml` samplesheet

    :param df: The data to convert
    :type df: `pd.DataFrame`
    :param output_cols: Which columns to output, defaults to `SAMPLESHEET_KEYS`
    :type output_cols: `Sequence[str]`, optional
    :param cols_as_str: Which columns to output as `str`s rather than `list[str]`, defaults to `[]`
    :type cols_as_str: `Collection[str]`, optional
    :param sortby: How to sort each record, defaults to `SAMPLESHEET_SORT_KEYS`
    :type sortby: `list[str] | str`, optional
    :param groupby: How to group records (so as to put a delimiter between different groups for easier visual parsing), defaults to `SAMPLESHEET_GROUP_KEY`
    :type groupby: `list[str] | str`, optional
    :param delimiter: The delimiter to use between groups, defaults to `'#'*80`
    :type delimiter: `str`, optional
    :return: The formatted `yml` samplesheet
    :rtype: `str`
    """
    # Drop unnecessary columns and sort by defined order
    to_keep = [col for col in output_cols if col in df.columns]
    df = df[to_keep].copy()

    # Convert lists to strings if desired and sort values
    for col in cols_as_str:
        df[col] = df[col].apply(lambda lst: lst[0] if len(lst) == 1 else lst)
    df.sort_values(sortby, inplace=True)

    # Add custom representer for sequences
    for sequence_type in (list, tuple):
        add_representer(data_type=sequence_type, representer=sequence_representer)

    # Generate output string
    yml = ''
    for _, group in df.groupby(groupby):
        # Convert to records and filter out nans
        records = group.to_dict(orient='records')
        records = [
            {key: value for key, value in rec.items() if is_valid(value)}
            for rec in records
        ]

        # Dump and write delimiter between groups
        yml += dump(
            data=records,
            Dumper=Dumper,
            default_flow_style=False,
            sort_keys=False,
        )
        yml += f'\n{delimiter}\n\n'

    return yml
