from collections.abc import Iterable
from dataclasses import MISSING, fields
from datetime import date
from re import findall
from typing import Any

from pandas import DataFrame, Series, isna
from rich.table import Table
from sqlalchemy import inspect, select
from sqlalchemy.orm import Mapper, Relationship, Session

from .orm.base import Base, Model


def get_format_string_vars(string: str) -> set[str]:
    pattern = r'{(\w+)(?:\[\d+\])?}'
    variables = set(findall(pattern, string))

    return variables


def rich_table(data: DataFrame, header: list[str] = []) -> Table:
    """_summary_

    :param data: _description_
    :type data: pd.DataFrame
    :param header: _description_, defaults to []
    :type header: list[str], optional
    :param message: _description_, defaults to ''
    :type message: str, optional
    """
    table = Table(*header)

    for idx, row in data.iterrows():
        table.add_row(str(idx), *(str(v) for v in row.values))

    return table


def construct_where_condition(
    attribute_name: str, value: Any, model_inspector: Mapper[Base]
):
    if '.' not in attribute_name:
        attribute = model_inspector.attrs[attribute_name].class_attribute
        return attribute.ilike(value) if isinstance(value, str) else attribute == value

    parent_name, parent_attribute_name = attribute_name.split('.', maxsplit=1)
    parent_inspector = model_inspector.relationships[parent_name].mapper
    parent = model_inspector.attrs[parent_name].class_attribute

    parent_where_condition = construct_where_condition(
        parent_attribute_name, value, model_inspector=parent_inspector
    )
    return parent.has(parent_where_condition)


def date_to_id(date_data: Series, prefix: str, id_length: int) -> str:
    index = date_data.name
    date_: date = date_data.iloc[0]

    return f'{prefix}{date_.strftime("%y")}{index:0{id_length - 4}}'


def model_from_data_columns(
    columns: Iterable[str], db_model_base_class: type[Base]
) -> type[Base]:
    db_models = {
        model.class_.__name__: model.class_
        for model in db_model_base_class.registry.mappers
    }
    model_names = {col.split('.')[0] for col in columns}
    model_name = model_names.pop()
    model = db_models[model_name]

    return model


def parent_models_from_data_columns(
    columns: Iterable[str], model: type[Base]
) -> dict[str, type[Base]]:
    inspector = inspect(model)
    parent_columns = {col.split('.')[1] for col in columns if col.count('.') > 1}

    return {col: inspector.relationships[col].mapper.class_ for col in parent_columns}


def model_init_fields(model: type[Base]) -> list[str]:
    return [field.name for field in fields(model) if field.init]


def required_model_init_fields(model: type[Base]) -> list[str]:
    return [
        field.name
        for field in fields(model)
        if field.init and field.default is MISSING and field.default_factory is MISSING
    ]


def construct_agg_funcs(model: type[Base], data_columns: Iterable[str]) -> dict:
    inspector = inspect(model)
    collection_classes = {
        col: inspector.relationships.get(col, Relationship()).collection_class
        for col in data_columns
    }

    return {
        col: 'first' if collection_class is None else collection_class
        for col, collection_class in collection_classes.items()
    }


def get_matching_obj(
    data: Series | dict, session: Session, model: type[Model]
) -> Model | None | bool:
    where_conditions = []

    model_field_names = (field.name for field in fields(model))
    cleaned_data = {
        col: val
        for col, val in data.items()
        if not isna(val) and isinstance(col, str) and col in model_field_names
    }

    for col, val in cleaned_data.items():
        inspector = inspect(model)
        where = construct_where_condition(col, value=val, model_inspector=inspector)
        where_conditions.append(where)

    if not where_conditions:
        return None

    stmt = select(model).where(*where_conditions)
    matches = session.execute(stmt).scalars().all()

    if len(matches) == 0:
        return None
    elif len(matches) > 1:
        return False

    return matches[0]
