"""
This module contains functions that are strung together in `main.py` to
create a command-line interface.

Functions:
    - `load_data`: Load data from a file, validating it against a schema
    
    - `map_libs_to_fastqdirs`: Go from a list of fastq directories to a
    mapping of library ID to fastq directory
    
    - `validate_dir`: Checks that a directory has required files
"""
from collections.abc import Collection
from json import load as load_json
from pathlib import Path
from re import match
from typing import Any

from jsonschema import ValidationError, validate
from rich import print as rprint
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from typer import Abort
from yaml import safe_load as safe_load_yml

from .defaults import LIBRARY_GLOB_PATTERN, SEE_MORE
from .utils import _load_csv


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


def validate_dir(direc: Path, required_files: Collection[Path] = []) -> dict[str, Path]:
    """
    Checks that `direc` has required files and returns them,
    creating `direc` if necessary.

    :param direc: Config directory to check
    :type direc: `pathlib.Path`
    :param required_files: Filenames required to exist in direc
    :type required_files: `list[pathlib.Path]`
    :raises `typer.Abort`: Raise error if required files not found in
    `direc`
    :return: A `dict` mapping the filename to its absolute path
    :rtype: `dict[str, pathlib.Path]`
    """
    # Create directory if it doesn't exist
    direc = direc.resolve(strict=True)
    direc.mkdir(exist_ok=True, parents=True)

    # Generate absolute paths for required files, find missing ones
    required_paths = {
        path.name: (direc / path).resolve(strict=True) for path in required_files
    }
    missing_files = '\n'.join(
        filename for filename, path in required_paths.items() if not path.exists()
    )

    if missing_files:
        rprint(
            f'Please place the following files in {direc}. {SEE_MORE}',
            missing_files,
            sep='\n',
        )
        raise Abort()

    return required_paths


def new_db_session(
    base_class: type[DeclarativeBase], **kwargs
) -> sessionmaker[Session]:
    """Create and return a new database session, initializing the
    database if necessary.

    :param base_class: The base class for the database to whose
    metadata the tables will be added.
    :type base_class: `type[sqlalchemy.orm.DeclarativeBase]`
    :param kwargs: Keyword arguments to pass to `sqlalchemy.URL.create`
    :return: A sessionmaker that can be used to create a new session.
    :rtype: sessionmaker[Session]
    """
    url = URL.create(**kwargs)
    engine = create_engine(url)
    Session = sessionmaker(engine)
    base_class.metadata.create_all(engine)

    return Session


# what's a better name for the function below?
def validate_str(string: str, pattern: str, string_name: str) -> str:
    if match(pattern, string=string) is None:
        rprint(
            f'The {string_name} [orange1]{string}[/] does not match '
            f'the pattern [green]{pattern}[/].'
        )
        raise Abort()

    return string
