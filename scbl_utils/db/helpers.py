from datetime import date
from re import findall
from typing import Any

from pandas import DataFrame, Series
from rich.table import Table
from sqlalchemy.orm import Mapper

from .orm.base import Base


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
