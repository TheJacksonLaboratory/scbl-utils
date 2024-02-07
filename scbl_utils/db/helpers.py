from re import findall

from pandas import DataFrame
from rich.table import Table


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
