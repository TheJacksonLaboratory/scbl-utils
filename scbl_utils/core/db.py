"""
This module contains functions related to database operations used in
`main.py` to create a command-line interface.

Functions:
    - `db_session`: Create and return a new database session,
    populating with tables if necessary

    - `matching_rows_from_table`: Get rows from a table that match
    certain criteria
"""
from itertools import zip_longest
from typing import Any, Hashable, Sequence

from rich import print as rprint
from rich.console import Console
from rich.table import Table
from sqlalchemy import URL, and_, create_engine, or_, select
from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute, Session, sessionmaker
from typer import Abort


def db_session(base_class: type[DeclarativeBase], **kwargs) -> sessionmaker[Session]:
    """Create and return a new database session, initializing the
    database if necessary.

    :param base_class: The base class for the database to whose
    metadata the tables will be added.
    :type base_class: `type[sqlalchemy.orm.DeclarativeBase]`
    :param kwargs: Keyword arguments to pass to `sqlalchemy.URL.create`
    :return: A sessionmaker that can be used to create a new session.
    :rtype: sessionmaker[Session]
    """
    url = URL.create(**kwargs)
    engine = create_engine(url)
    Session = sessionmaker(engine)
    base_class.metadata.create_all(engine)

    return Session


def matching_rows_from_table(
    session: Session,
    model: type[DeclarativeBase],
    model_attribute_to_data_col: dict[str, str],
    data: list[dict[str, Any]],
    data_filename: str,
) -> Sequence:
    """Get rows from a table that match the specified columnds of `data`.

    :param session: A database session that has been begun
    :type session: `sqlalchemy.Session`
    :param model: The model class for the table
    :type model: `type[scbl_utils.db_models.bases.Base]`
    :param model_attribute_to_data_col: A mapping from model attributes
    to the column names in the data. For example, we want to get all
    people who match the first name and last name in a list of labs, we
    would pass in
    `{'first_name': 'pi_first_name', 'last_name': 'pi_last_name'}`
    :type model_attribute_to_data_col:
    `dict[str, str]`
    :param data: A list of dicts representing the data to match
    :type data: `list[dict[str, Any]]`
    :param data_filename: The name of the CSV file that the data comes
    from. Used for error reporting.
    :type data_filename: `str`
    :raises Abort: If the table contains no rows matching the filter,
    raise error
    :return: A list of rows from the table that match the filter dicts
    :rtype: `list`
    """
    stmts = [
        select(model).filter_by(
            **{
                model_att: record[col]
                for model_att, col in model_attribute_to_data_col.items()
            }
        )
        for record in data
    ]
    found_rows = [session.execute(stmt).scalar() for stmt in stmts]

    missing = [
        [str(v) for k, v in record.items() if k in model_attribute_to_data_col.values()]
        for record, obj in zip_longest(data, found_rows)
        if obj is None
    ]

    if not missing:
        return found_rows

    headers = data[0].keys()
    error_table = Table(*headers)

    for values in missing:
        error_table.add_row(*values)

    console = Console()
    console.print(
        f'The table [green]{model.__tablename__}[/] contained no '
        'rows matching the table below, which was found in [orange1]'
        f'{data_filename}[/]',
        error_table,
        sep='\n',
    )

    raise Abort()
