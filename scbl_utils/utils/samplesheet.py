from pathlib import Path

import pandas as pd
from yaml import Dumper, SequenceNode

from .defaults import (
    LIB_TYPES_TO_PROGRAM,
    LIBRARY_GLOB_PATTERN,
    SAMPLESHEET_KEY_TO_TYPE,
    SIBLING_REPOSITORY,
)


def map_libs_to_fastqdirs(
    fastqdirs: list[Path], glob_pattern: str = LIBRARY_GLOB_PATTERN
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
        raise FileNotFoundError(
            f'The following directories did not contain any files that match glob pattern {glob_pattern}.\n{bad_dirs}'
        )

    return lib_to_fastqdir


def get_program_from_lib_types(
    sample_df: pd.DataFrame,
    lib_types_to_program: list[dict[str, list[str] | str]] = LIB_TYPES_TO_PROGRAM,
    samplesheet_key_to_type: dict[str, type] = SAMPLESHEET_KEY_TO_TYPE,
) -> pd.Series:
    """To be used as first argument to `samplesheet_df.groupby('sample_name').agg`. Aggregates `sample_df` (representing one sample), adding information derived from the library types

    :param sample_df: This dataframe represents one sample, with each row representing a library belonging to that sample. It must contain the column 'library_types'
    :type sample_df: `pd.DataFrame`
    :param lib_types_to_program: A list of dicts that associates a library type combo to a tool-command-reference_dir combo, defaults to LIB_TYPES_TO_PROGRAM
    :type lib_types_to_program: list[dict[str, list[str]  |  str]], optional
    :param samplesheet_key_to_type: A mapping between each samplesheet key and its type, defaults to SAMPLESHEET_KEY_TO_TYPE
    :type samplesheet_key_to_type: `dict[str, type]`, optional
    :raises ValueError: If the combination of library types for a given sample doesn't exist, raises error
    :return: A `dict` that compresses the many rows of `sample_df` into one row, which will be handled by `samplesheet.groupby('sample_name').agg`
    :rtype: `dict[str, list[str] | bool | str]`
    """
    # Get a list of the mappings that have the same library type combo
    # as this sample. It should be only one
    lib_dicts = [
        lib_dict
        for lib_dict in lib_types_to_program
        if set(lib_dict['library_types']) == set(sample_df['library_types'])
    ]
    if len(lib_dicts) != 1:
        valid_lib_type_combos = '\n'.join(
            str(lib_spec['library_types']) for lib_spec in lib_types_to_program
        )
        raise ValueError(
            f'\n{sample_df}\nThe combination of library types in the table above is not a valid combination. Valid combinations shown below:\n{valid_lib_type_combos}'
        )

    # Get the one dict containing information about this combination of
    # libraries and filter to prevent over-writing columns in sample_df
    lib_dict = lib_dicts[0]
    lib_dict_filtered = {
        key: value for key, value in lib_dict.items() if key not in sample_df.columns
    }.copy()

    # Fill sample_df with tool, command, and reference_dir based on its
    # combination of library types
    sample_df_filled = sample_df.assign(**lib_dict_filtered)

    # Iterate over each column of sample_df_filled and aggregate
    aggregated = pd.Series()
    for col, series in sample_df_filled.items():
        if col == 'n_cells':
            aggregated[col] = series.max()
        elif samplesheet_key_to_type.get(col) == list[str] or series.nunique() > 1:  # type: ignore
            aggregated[col] = series.to_list()
        else:
            aggregated[col] = series.drop_duplicates().item()

    return aggregated


def get_latest_tool_version(
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


def list_representer(dumper: Dumper, data: list) -> SequenceNode:
    return dumper.represent_sequence(tag='tag:yaml.org,2002:seq', sequence=data, flow_style=True)
