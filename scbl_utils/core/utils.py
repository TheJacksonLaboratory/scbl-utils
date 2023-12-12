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
from typing import Any

from numpy import nan
from pandas import read_csv
from yaml import Dumper, SequenceNode

# TODO: eventually remove dependency on pandas for performance?
def _load_csv(f: TextIOWrapper) -> list[dict[Hashable, Any]]:
    """
    Load a CSV file into a list of dicts, replacing empty strings
    with `None`

    :param f: Opened file to load
    :type f: `io.TextIOWrapper`
    :return: A list of dicts representing the rows of the CSV file
    :rtype: `list[dict[str, Any]]`
    """
    data = read_csv(f)
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