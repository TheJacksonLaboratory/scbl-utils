"""
This module contains functions related to data validation used in
`main.py` to create a command-line interface.

Functions:
    - `validate_dir`: Checks that a directory has required files

    - `validate_str`: Validate a string against a pattern
"""
from re import match
from scbl_utils.defaults import SEE_MORE


from rich import print as rprint
from typer import Abort


from collections.abc import Collection
from pathlib import Path


def validate_dir(
    direc: Path,
    required_files: Collection[Path] = [],
    create: bool = True,
    error_prefix: str | None = None,
) -> dict[str, Path]:
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
    # Create directory if desired
    if create:
        direc = direc.resolve(strict=True)
        direc.mkdir(exist_ok=True, parents=True)

    # Generate absolute paths for required files, find missing ones
    required_paths = {path.name: (direc / path).resolve() for path in required_files}
    missing_files = '\n'.join(
        filename for filename, path in required_paths.items() if not path.exists()
    )

    main_error = f'Please add the following paths to [orange1]{direc}[/]. {SEE_MORE}'
    full_error = (
        f'{error_prefix} {main_error}' if error_prefix is not None else main_error
    )

    if missing_files:
        rprint(full_error, missing_files, sep='\n')
        raise Abort()

    return required_paths


# what's a better name for the function below?
def validate_str(string: str, pattern: str, string_name: str):
    if match(pattern, string=string) is None:
        rprint(
            f'The {string_name} [orange1]{string}[/] does not match '
            f'the pattern [green]{pattern}[/].'
        )
        raise Abort()

    return string