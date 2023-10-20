from pathlib import Path

from .defaults import DOCUMENTATION


def config_dir(direc: Path, required_files: list[Path]) -> dict[str, Path]:
    """Checks that config_dir has required files and returns them

    :param direc: Config dir to check
    :type direc: Path
    :param required_files: Filenames required to exist in config_dir
    :type required_files: list[Path]
    :raises FileNotFoundError: Raise error if required files not found in config_dir
    :return: A dict mapping the filename to its absolute path
    :rtype: dict[str, Path]
    """
    # Generate absolute paths that should be in config_dir and get the
    # nonexistent ones.
    required_paths = {path.name: (direc / path).absolute() for path in required_files}
    missing_files = '\n'.join(
        filename for filename, path in required_paths.items() if not path.exists()
    )

    if missing_files:
        raise FileNotFoundError(
            f'Please place the following files in {direc.absolute()}. See {DOCUMENTATION} for more details.\n{missing_files}'
        )

    return required_paths
