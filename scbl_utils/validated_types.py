from functools import cache
from typing import Annotated

import polars as pl
from pydantic import AfterValidator, StringConstraints
from scbl_db import ORDERED_MODELS, Base
from scbl_db.bases import Base
from sqlalchemy import inspect


@cache
def _validate_model_field(model: type[Base], field: str):
    if field.count('.') == 0:
        if field not in model.dc_field_names():
            raise ValueError(f'{field} is not a field of {model.__name__}')

        return

    parent_name, parent_field = field.split('.', maxsplit=1)
    relationships = inspect(model).relationships

    if parent_name not in relationships.keys():
        raise ValueError(f'{parent_name} is not a relationship in {model.__name__}')

    parent_model = relationships[parent_name].mapper.class_
    _validate_model_field(parent_model, field=parent_field)


@cache
def _validate_db_target(db_target: str) -> str:
    model_name, field = db_target.split('.', maxsplit=1)
    model = ORDERED_MODELS[model_name]

    _validate_model_field(model, field=field)

    return db_target


DBModelName = Annotated[
    str, StringConstraints(pattern=rf'^{"|".join(ORDERED_MODELS.keys())}$')
]
DBTarget = Annotated[
    str,
    StringConstraints(pattern=rf'({"|".join(ORDERED_MODELS.keys())})\.[\w.]+'),
    AfterValidator(_validate_db_target),
]


def _validate_type_string(string: str):
    valid_types = {
        'bool': bool,
        'float': float,
        'int': int,
        'str': str,
        'date': pl.Date,
    }

    if string not in valid_types:
        raise ValueError(
            f'Type-defining string must be one of {valid_types}, not {string}'
        )

    return valid_types[string]


TypeString = Annotated[str, AfterValidator(_validate_type_string)]
