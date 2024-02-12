from collections.abc import Iterable
from dataclasses import MISSING, fields
from pathlib import Path

from .db.orm.base import Base


def validate_db_target(target: str, base_class: type[Base]) -> None:
    ...


def validate_data_columns(
    columns: Iterable[str], db_model_base_class: type[Base], data_source: str
) -> None:
    model_names = {col.split('.')[0] for col in columns}
    if len(model_names) != 1:
        raise ValueError(
            f'The data in [orange1]{data_source}[/] must represent only one table in the database, but {model_names} were found'
        )

    model_name = model_names.pop()
    valid_model_names = {
        model.class_.__name__: model.class_
        for model in db_model_base_class.registry.mappers
    }
    if model_name not in valid_model_names:
        raise ValueError(
            f'[orange1]{model_name}[/] from [orange1]{data_source}[/] does not exist'
        )

    model = valid_model_names[model_name]
    required_model_init_fields = {
        field.name: field
        for field in fields(model)
        if field.init
        if field.default is MISSING and field.default_factory is MISSING
    }

    renamed_data_columns = {col.split('.')[1] for col in columns}
    missing_fields = ', '.join(required_model_init_fields.keys() - renamed_data_columns)

    if missing_fields:
        raise ValueError(
            f'The following fields are required to initialize a [green]{model_name}[/], but are missing from the columns of [orange1]{data_source}[/]: [green]{missing_fields}[/]'
        )


def validate_directory(
    directory: Path,
    required_directories: Iterable[str | Path] = [],
    required_files: Iterable[str | Path] = [],
) -> None:
    if not directory.is_dir():
        raise NotADirectoryError(f'{directory} is not a directory.')

    missing_directories = [
        str(directory / dirname)
        for dirname in required_directories
        if not (directory / dirname).is_dir()
    ]
    missing_files = [
        str(directory / filename)
        for filename in required_files
        if not (directory / filename).is_file()
    ]

    if missing_directories:
        raise NotADirectoryError(
            f'The following directories are missing from {directory}: {missing_directories}'
        )

    if missing_files:
        raise FileNotFoundError(
            f'The following files are missing from {directory}: {missing_files}'
        )
