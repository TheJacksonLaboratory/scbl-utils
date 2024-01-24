"""
This module contains utility functions that are uses by other submodules
in the `scbl-utils` package.

Functions:
    - `_load_csv`: Load a CSV file into a list of dicts, replacing empty
    strings with `None`'

    - `_sequence_representer`: Representer for `yaml` that allows for
    sequences of sequences to be represented as a single line
"""
from collections.abc import Collection, Hashable
from io import TextIOWrapper
from re import findall
from typing import Any

import pandas as pd
from numpy import nan
from rich import print as rprint
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table
from sqlalchemy import inspect, select
from sqlalchemy.orm import InstrumentedAttribute, Session
from yaml import Dumper, SequenceNode

from ..db_models.bases import Base
from ..defaults import OBJECT_SEP_CHAR


def _load_csv(f: TextIOWrapper) -> list[dict[Hashable, Any]]:
    """
    Load a CSV file into a list of dicts, replacing empty strings
    with `None`

    :param f: Opened file to load
    :type f: `io.TextIOWrapper`
    :return: A list of dicts representing the rows of the CSV file
    :rtype: `list[dict[str, Any]]`
    """
    data = pd.read_csv(f)
    data.replace(nan, None, inplace=True)
    return data.to_dict(orient='records')


def _sequence_representer(dumper: Dumper, data: list | tuple) -> SequenceNode:
    """
    Representer for `yaml` that allows for sequences of sequences to be
    represented as a single line

    :param dumper: The `yml` dumper to register the representer with
    :type dumper: `yaml.Dumper`
    :param data: The data to represent
    :type data: `list` | `tuple`
    :return: A `yaml.SequenceNode` representing the data
    :rtype: `yaml.SequenceNode`
    """
    # Get the first item in the sequence and check its type
    item = data[0]
    if isinstance(item, Collection) and not isinstance(item, str):
        return dumper.represent_list(data)
    else:
        return dumper.represent_sequence(
            tag='tag:yaml.org,2002:seq', sequence=data, flow_style=True
        )


def _get_format_string_vars(string: str) -> set[str]:
    pattern = r'{(\w+)(?:\[\d+\])?}'
    variables = set(findall(pattern, string))

    return variables


# TODO: probably move this into db module
def _get_matching_obj(
    data: pd.Series, session: Session, model: type[Base]
) -> Base | None | bool:
    where_conditions = []

    excessively_nested_cols = {
        col for col in data.keys() if col.count(OBJECT_SEP_CHAR) > 1
    }
    if excessively_nested_cols:
        rprint(
            f'While trying to retrieve a [green]{model.__tablename__}[/] that matches the data row shown below, the columns [orange]{excessively_nested_cols}[/] will be excluded from the query because they require matching an attribute of a parent of a parent of a [green]{model.__tablename__}[/], which is currently not supported.',
            f'[orange]{data.to_dict()}[/]',
            sep='\n\n',
        )

    # TODO: could this be sped up with a neat vectorized function
    cleaned_data = {
        col: val for col, val in data.items() if col not in excessively_nested_cols
    }
    for col, val in cleaned_data.items():
        if not isinstance(col, str) or val is None:
            continue

        inspector = inspect(model)
        if OBJECT_SEP_CHAR in col:
            parent_name, parent_att_name = col.split(OBJECT_SEP_CHAR)
            parent_model: type[Base] = (
                inspect(model).relationships[parent_name].mapper.class_
            )

            parent = inspector.attrs[parent_name].class_attribute
            parent_inspector = inspect(parent_model)
            parent_att = parent_inspector.attrs[parent_att_name].class_attribute

            where = (
                parent.has(parent_att.ilike(val))
                if isinstance(val, str)
                else parent.has(parent_att == val)
            )
        else:
            att = inspector.attrs[col].class_attribute
            where = att.ilike(val) if isinstance(val, str) else att == val

        where_conditions.append(where)

    # TODO: this assumes that there is only one unique match in the table
    stmt = select(model).where(*where_conditions)
    matches = session.execute(stmt).scalars().all()

    if len(matches) == 0:
        return None
    elif len(matches) > 1:
        return False

    return matches[0]


def _print_table(
    data: pd.DataFrame, console: Console, header: list[str] = [], message: str = ''
) -> None:
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

    if table.row_count > 0:
        console.print(message, table, sep='\n')
