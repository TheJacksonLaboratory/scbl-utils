from collections.abc import Collection, Sequence
from pathlib import Path
from re import sub, match

from numpy import nan
import pandas as pd
from rich import print as rprint
from typer import Abort
from yaml import Dumper, SequenceNode, add_representer, dump

from .defaults import (
    ANTIBODY_LIB_TYPES,
    LIBRARY_GLOB_PATTERN,
    PLATFORMS_TO_PROBESET,
    SAMPLENAME_BLACKLIST_PATTERN,
    SEP_PATTERN,
    SIBLING_REPOSITORY,
    SAMPLESHEET_KEYS,
    SAMPLESHEET_SORT_KEYS,
    SAMPLESHEET_GROUP_KEY,
    VISIUM_DIR
)


def map_libs_to_fastqdirs(
    fastqdirs: Collection[Path], glob_pattern: str = LIBRARY_GLOB_PATTERN
) -> dict[str, str]:
    """Go from a list of fastq dirs to a mapping of library ID to fastq dir

    :param fastq_dirs: list of fastq dirs
    :type fastq_dirs: list[Path]
    :param glob_pattern: pattern to glob for in each fastq dir, defaults to LIBRARY_GLOB_PATTERN
    :type glob_pattern: str, optional
    :raises FileNotFoundError: If any of the dirs do not contain files matching the glob pattern, raise FileNotFoundError
    :return: A dict mapping
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
        raise Abort()

    # Sort dict before returning
    sorted_libs = sorted(lib_to_fastqdir.keys())
    sorted_lib_to_fastqdir = {lib: lib_to_fastqdir[lib] for lib in sorted_libs}
    return sorted_lib_to_fastqdir


# def program_from_types(library_types: tuple[str, ...], lib_types_to_program: dict[tuple[str, ...], tuple[str, str, list[str]]] = LIB_TYPES_TO_PROGRAM) -> dict[str, str]:
#     sorted_lib_types = sorted(library_types)
#     sorted_lib_types = tuple(sorted_lib_types)

#     tool_command_refdir = lib_types_to_program.get(library_types)

#     if not tool_command_refdir:
#         sample_name = sample_df['sample_name'].iloc[0]
#         rprint(
#             f'The library types [bold orange1]{library_types}[/] associated with [bold orange1]{sample_name}[/] are not a valid combination. Valid combinations:\n[bold blue]',
#             *lib_types_to_program.keys(),
#             sep='\n',
#         )
#         raise Abort()


# def program_from_lib_types(
#     sample_df: pd.DataFrame,
#     ref_parent_dir: Path = REF_PARENT_DIR,
#     lib_types_to_program: dict[tuple[str, ...], tuple[str, str, list[str]]] = LIB_TYPES_TO_PROGRAM,  # type: ignore
#     samplesheet_key_to_type: dict[str, type] = SAMPLESHEET_KEY_TO_TYPE,
# ) -> pd.Series:
#     """To be used as first argument to `samplesheet_df.groupby('sample_name', as_index=False).apply`. Aggregates `sample_df` (representing one sample), adding information derived from the library types

#     :param sample_df: This dataframe represents one sample, with each row representing a library belonging to that sample. It must contain the column 'library_types'
#     :type sample_df: `pd.DataFrame`
#     :param lib_types_to_program: A dict that associates a library type combo to a tool-command-reference_dir combo, defaults to LIB_TYPES_TO_PROGRAM
#     :type lib_types_to_program: dict[tuple[str, ...], tuple[str, str, Path]
#     :param samplesheet_key_to_type: A mapping between each samplesheet key and its type, defaults to SAMPLESHEET_KEY_TO_TYPE
#     :type samplesheet_key_to_type: `dict[str, type]`, optional
#     :raises ValueError: If the combination of library types for a given sample doesn't exist, raises error
#     :return: A `dict` that compresses the many rows of `sample_df` into one row, which will be handled by `samplesheet.groupby('sample_name').agg`
#     :rtype: `pandas.Series`
#     """
#     # Initialize the output, a series aggreggating the df
#     aggregated = pd.Series()
#     aggregated['libraries'] = tuple(sample_df.index)

#     # Get the tool-command-refdir combo based on library types
#     # and throw error if not found
#     library_types = tuple(sample_df['library_types'].sort_values())
#     tool_command_refdir = lib_types_to_program.get(library_types)

#     if not tool_command_refdir:
#         sample_name = sample_df['sample_name'].iloc[0]
#         rprint(
#             f'The library types [bold orange1]{library_types}[/] associated with [bold orange1]{sample_name}[/] are not a valid combination. Valid combinations:\n[bold blue]',
#             *lib_types_to_program.keys(),
#             sep='\n',
#         )
#         raise Abort()

#     (
#         aggregated['tool'],
#         aggregated['command'],
#         aggregated['reference_dirs'],
#     ) = tool_command_refdir

#     # The list of reference dirs contains relative paths, make them
#     # absolute
#     aggregated['reference_dirs'] = [
#         ref_parent_dir / ref_child_dir for ref_child_dir in aggregated['reference_dirs']
#     ]

#     # Iterate over each column of sample_df and aggregate
#     for col, series in sample_df.items():
#         if col == 'n_cells':
#             aggregated[col] = series.max()
#         # TODO: think of a better way to check for 10x_platform
#         elif samplesheet_key_to_type.get(col) == list[str] or series.nunique() > 1 or col == '10x_platform':  # type: ignore
#             aggregated[col] = tuple(series)
#         else:
#             aggregated[col] = series.iloc[0]

#     return aggregated


def get_latest_version(
    tool: str, repository_link: str = SIBLING_REPOSITORY
) -> (
    str
):  # TODO: eventually, create a more easily parsable file in the github repo that has this information.
    """Get latest version of a given tool

    :param tool: The name of the tool
    :type tool: `str`
    :param repository_link: Link to repository containing a table to read information from, defaults to SIBLING_REPOSITORY
    :type repository_link: `str`, optional
    :return: The latest version of the tool
    :rtype: `str`
    """
    # Read the table from the README, rename columns, and format tool
    df = pd.read_html(repository_link)[0]
    df.rename(columns={col: col.lower() for col in df.columns}, inplace=True)
    df['tool'] = df['tool'].str.replace(' ', '-').str.lower()

    # Get the index of each row that actually has a tool name because
    # some rows are blank
    tool_idxs = df.dropna(subset='tool').index

    # Append the index of the last row, as well as 1 + that index. This
    # is relevant for the next step
    tool_idxs = tool_idxs.append(pd.Index([df.index[-1], df.index[-1] + 1]))

    # Pair each tool index with the index of the next row containing a tool name
    idx_pairings = zip(tool_idxs, tool_idxs[1:])

    # Fill the table so that there are no blank rows
    for first_row_with_tool, last_row_with_tool in idx_pairings:
        df.loc[range(first_row_with_tool, last_row_with_tool), 'tool'] = df.loc[first_row_with_tool, 'tool']  # type: ignore

    # Group by tool, get latest version for each tool, and format
    grouped = df.groupby('tool')
    latest_versions = grouped['version'].max()

    return latest_versions[tool]


def genomes_from_user(
    message: str, reference_dirs: Collection[Path], sample_name: str
) -> list[str]:
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
            f'Choose a genome in [bold green]{ref_dir.absolute()}[/] for [bold orange1]{sample_name}[/] ->',
            choices=genome_choices,
        )
        full_ref_path = str((ref_dir / genome).absolute())
        reference_paths.append(full_ref_path)

    return reference_paths


def get_antibody_tags(
    library_types: Collection[str], antibody_lib_types: set[str] = ANTIBODY_LIB_TYPES
) -> tuple[str, ...] | float:
    if not antibody_lib_types & set(library_types):
        return nan

    tags_df = pd.read_csv(
        'https://raw.githubusercontent.com/TheJacksonLaboratory/nf-tenx/main/assets/totalseq-b_universal.csv'
    )

    return tuple(tags_df['tag_id'])


def map_platform_to_probeset(
    df_row: pd.Series,
    platform_to_probset: dict[str, dict[str, str]] = PLATFORMS_TO_PROBESET,
):
    platforms = df_row['10x_platform']
    ref_paths = df_row['reference_path']
    for platform in platforms:
        probeset_dict = platform_to_probset.get(platform)
        if not probeset_dict:
            continue
        genome = ref_paths[0].split('/')[-1]
        probe_set = probeset_dict.get(genome)
        if probe_set:
            return probe_set

    return nan


def get_visium_info(
    df_row: pd.Series,
    visium_dir: Path = VISIUM_DIR,
) -> pd.Series:
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


def sanitize_samplename(sample_name: str) -> str:
    legal = sub(pattern=SAMPLENAME_BLACKLIST_PATTERN, repl='', string=sample_name)
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


def samplesheet_from_df(
    df: pd.DataFrame,
    output_cols: Sequence[str] = SAMPLESHEET_KEYS,
    cols_as_str: Collection[str] = [],
    sortby: list[str] | str = SAMPLESHEET_SORT_KEYS,
    groupby: list[str] | str = SAMPLESHEET_GROUP_KEY,
    delimiter: str = '#' * 80,
) -> str:
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
            {key: value for key, value in rec.items() if value == value}
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
