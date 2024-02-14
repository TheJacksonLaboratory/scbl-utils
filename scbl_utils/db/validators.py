from collections.abc import Iterable
from dataclasses import fields

from sqlalchemy import inspect

from .orm.base import Base


def validate_model_target(model: type[Base], target: str, data_source: str) -> None:
    if '.' not in target:
        if not target in {field.name for field in fields(model)}:
            raise ValueError(
                f'{target} is not a column in {model.__name__} from {data_source}'
            )
        return

    parent_name, parent_field = target.split('.', maxsplit=1)
    if parent_name not in inspect(model).relationships:
        raise ValueError(f'{parent_name} is not a relationship in {model.__name__}')

    parent_model = inspect(model).relationships[parent_name].mapper.class_

    return validate_model_target(parent_model, parent_field, data_source=data_source)


def validate_data_columns(
    columns: Iterable[str], db_model_base_class: type[Base], data_source: str
) -> None:
    if not all('.' in col for col in columns):
        raise ValueError(
            f'All columns in [orange1]{data_source}[/] must contain a period (.) to separate the model name from the column name.'
        )

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
    for col in columns:
        col = col.split('.', maxsplit=1)[1]
        validate_model_target(model, col, data_source=data_source)
