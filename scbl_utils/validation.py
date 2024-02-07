from pathlib import Path
from collections.abc import Iterable
def validate_directory(directory: Path, required_directories: Iterable[str | Path] = [], required_files: Iterable[str | Path] = []) -> None:
    if not directory.is_dir():
        raise NotADirectoryError(directory)
    
    missing_directories = [str(directory / dirname) for dirname in required_directories if not (directory / dirname).is_dir()]
    missing_files = [str(directory / filename) for filename in required_files if not (directory / filename).is_file()]

    if missing_directories:
        raise NotADirectoryError(missing_directories)
    
    if missing_files:
        raise FileNotFoundError(missing_files)
    