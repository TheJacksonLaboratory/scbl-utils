"""
This module contains functions related to database operations used in
`main.py` to create a command-line interface.

Functions:
    - `db_session`: Create and return a new database session,
    populating with tables if necessary

    - `matching_rows_from_table`: Get rows from a table that match
    certain criteria
"""
from rich.console import Console
from rich.table import Table
from typing import Any
from sqlalchemy import URL, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from typer import Abort


def db_session(
    base_class: type[DeclarativeBase], **kwargs
) -> sessionmaker[Session]:
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
    model,
    filter_dicts: list[dict[str, Any]],
    data_filename: str,
) -> list:
    """Get rows from a table that match the filter dicts.

    :param session: A database session that has been begun
    :type session: `sqlalchemy.Session`
    :param model: The model class for the table
    :type model: `type[scbl_utils.db_models.bases.Base]`
    :param filter_dicts: A list of dicts where each dict has keys that
    are columns and values that are the values to filter by
    :type filter_dicts: `list[dict[str, Any]]`
    :param data_filename: The name of the CSV file that the data comes
    from. Used for error reporting.
    :type data_filename: `str`
    :raises Abort: If the table contains no rows matching the filter,
    raise
    :return: A list of rows from the table that match the filter dicts
    :rtype: `list`
    """
    stmts = [select(model).filter_by(**filter_dict) for filter_dict in filter_dicts]
    found_rows = [session.execute(stmt).scalar() for stmt in stmts]

    missing = [
        filter_dict.values()
        for filter_dict, obj in zip(filter_dicts, found_rows)
        if obj is None
    ]

    if not missing:
        return found_rows  # type: ignore

    headers = filter_dicts[0].keys()
    table = Table(*headers)

    for values in missing:
        table.add_row(*values)

    console = Console()
    console.print(
        f'The table [green]{model.__tablename__.capitalize()}[/] contained no '
        'rows matching the table below, which was found in [orange1]'
        f'{data_filename}[/]',
        table,
        sep='\n',
    )

    raise Abort()