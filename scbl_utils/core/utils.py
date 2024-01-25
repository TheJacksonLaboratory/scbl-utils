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
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table
from sqlalchemy.orm import InstrumentedAttribute
from yaml import Dumper, SequenceNode


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
