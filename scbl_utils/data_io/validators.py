from collections.abc import Iterable, Mapping
from pathlib import Path

type DirectoryStructure = Mapping[
    str | Path, Iterable[str | Path] | 'DirectoryStructure'
] | Iterable[str | Path]


def validate_directory(
    directory: str | Path, required_structure: DirectoryStructure
) -> None:
    directory = Path(directory)

    if not directory.is_dir():
        raise NotADirectoryError(f'{directory} is not a directory.')

    if isinstance(required_structure, Mapping):
        for sub_dirname, sub_structure in required_structure.items():
            validate_directory(
                directory / sub_dirname, required_structure=sub_structure
            )
        return

    if isinstance(required_structure, Iterable):
        missing_files = {
            str(directory / filename)
            for filename in required_structure
            if not (directory / filename).is_file()
        }
        if missing_files:
            raise FileNotFoundError(
                f'The following files are missing from {directory}: {missing_files}'
            )
        return

    raise TypeError(
        f'{required_structure} must be a [green]Mapping[/] or an [green]Iterable[/], not a [orange1]{type(required_structure).__name__}[/]'
    )
