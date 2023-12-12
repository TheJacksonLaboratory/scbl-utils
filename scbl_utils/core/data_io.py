"""
This module contains functions related to data input/output that are
used in `main.py` to create a command-line interface.

Functions:
    - `load_data`: Load data from a file, validating it against a schema
    
    - `map_libs_to_fastqdirs`: Go from a list of fastq directories to a
    mapping of library ID to fastq directory
"""

from collections.abc import Collection
from scbl_utils.defaults import LIBRARY_GLOB_PATTERN, SEE_MORE
from .utils import _load_csv


from jsonschema import ValidationError, validate
from rich import print as rprint
from typer import Abort
from yaml import safe_load as safe_load_yml


from json import load as load_json
from pathlib import Path
from typing import Any


def load_data(data_path: Path, schema: dict = {}) -> Any:
    """Load data from a file, validating it against a schema

    :param data_path: Path to the file to load
    :type data_path: `pathlib.Path`
    :param schema: JSON schema to validate the data against, defaults
    to `{}`
    :type schema: `dict`, optional
    :raises `typer.Abort`: If the file extension is not supported or the data
    is invalid against the schema, raise error
    :return: The loaded data
    :rtype: Any
    """
    # Define loaders for different file extensions and resolve data path
    loaders = {'.csv': _load_csv, '.json': load_json, '.yml': safe_load_yml}
    data_path = data_path.resolve(strict=True)

    # Make sure file extension is supported
    if data_path.suffix not in loaders:
        rprint(
            f'File extension [orange1]{data_path.suffix}[/] of [orange1]{data_path}[/] not yet supported.'
        )
        raise Abort()

    # Load data and validate against schema
    with data_path.open() as f:
        loading_func = loaders[data_path.suffix]
        data = loading_func(f)

    try:
        validate(data, schema=schema)
    except ValidationError as e:
        rprint(
            f'[orange1]{data_path}[/] is incorrectly formatted. {SEE_MORE}', e, sep='\n'
        )
        raise Abort()
    else:
        return data


def map_libs_to_fastqdirs(
    fastqdirs: Collection[Path], glob_pattern: str = LIBRARY_GLOB_PATTERN
) -> dict[str, str]:
    """
    Go from a list of fastq directories to a mapping of library ID
    to fastq directory.

    :param fastqdirs: list of fastq dirs
    :type fastqdirs: `list[pathlib.Path]`
    :param glob_pattern: pattern to glob for in each fastq directory,
    defaults to `defaults.LIBRARY_GLOB_PATTERN`
    :type glob_pattern: `str`, optional
    :return: A `dict` mapping each library ID to its fastq directory as
    a string of its absolute path
    :rtype: `dict[str, str]`
    """
    # Get a mapping between library names and the fastq directories
    # they're in. Assumes a specific format for filenames
    lib_to_fastqdir = {
        path.name.split('_')[0]: str(fastqdir.resolve(strict=True))
        for fastqdir in fastqdirs
        for path in fastqdir.glob(glob_pattern)
    }

    # Get the directories that do not contain any files matching the
    # glob pattern. If there are any, raise error
    input_dirs = {str(fastqdir.resolve(strict=True)) for fastqdir in fastqdirs}
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