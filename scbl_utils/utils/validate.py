from pathlib import Path

from rich import print as rprint
from typer import Abort

from .defaults import DOCUMENTATION


def direc(direc: Path, required_files: list[Path] = []) -> dict[str, Path]:
    """Checks that direc has required files and returns them, creating direc in the process

    :param direc: Config dir to check
    :type direc: Path
    :param required_files: Filenames required to exist in direc
    :type required_files: list[Path]
    :raises FileNotFoundError: Raise error if required files not found in direc
    :return: A dict mapping the filename to its absolute path
    :rtype: dict[str, Path]
    """
    # Create directory if it doesn't exist
    direc.mkdir(exist_ok=True, parents=True)

    # Generate absolute paths for required files, find missing ones
    required_paths = {path.name: (direc / path).absolute() for path in required_files}
    missing_files = '\n'.join(
        filename for filename, path in required_paths.items() if not path.exists()
    )

    if missing_files:
        rprint(
            f'Please place the following files in {direc.absolute()}. See {DOCUMENTATION} for more details.\n{missing_files}'
        )
        raise Abort()

    return required_paths
