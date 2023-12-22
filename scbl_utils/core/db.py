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
from typing import Any, Hashable, Literal

import pandas as pd
from numpy import rec
from rich import print as rprint
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table
from sqlalchemy import URL, create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute, Session, sessionmaker
from typer import Abort

from ..db_models.bases import Base
from ..defaults import DOCUMENTATION
from .utils import _get_user_input


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


def get_matching_rows_from_db(
    session: Session,
    data_col_to_model_attribute: dict[str, InstrumentedAttribute],
    data: list[dict[Hashable, Any]],
    data_source: str,
    raise_error: bool = True,
    return_unique: bool = True,
) -> list[dict[Hashable, Any]]:  # TODO: update this docstring
    """Get rows from a table that match the specified columns of `data`.

    :param session: An open session
    :type session: `sqlalchemy.orm.Session`
    :param model: The model class for the table
    :type model: `type[scbl_utils.db_models.bases.Base]`
    :param data_col_to_model_attribute: A mapping from model attributes
    to the column names in the data. See documentation for examples.
    :type data_col_to_model_attribute:
    `dict[InstrumentedAttribute, str]`
    :param data: A list of dicts representing the data to match
    :type data: `list[dict[str, Any]]`
    :param data_source: The name of the CSV file that the data comes
    from. Used for error reporting.
    :type data_source_name: `str`
    :param raise_error: Whether to raise an error if some rows are not
    found
    :type raise_error: `bool`
    :raises Abort: If the table contains no rows matching the filter,
    raise error
    :return: A list of rows from the table that match the data passed
    in. This list is the same length as the data passes in. Any rows of
    the table that do not match the data will be `None`.
    :rtype: `list`
    """
    models = {
        model_att.parent.class_ for model_att in data_col_to_model_attribute.values()
    }

    if len(models) != 1:
        rprint(
            'The data_col_to_model_attribute dict must contain '
            'attributes from only one model, but '
            f'[green]{models}[/] were given.'
        )
        raise Abort()

    model: type[Base] = models.pop()

    renamed_data = [{data_col_to_model_attribute.get(col, col): value for col, value in row.items()} for row in data]  # type: ignore
    unique_data_rows: list[dict[InstrumentedAttribute | str, Any]] = []

    # TODO: eventually use some slick set comprehension and adjust the
    # later code
    for row in renamed_data:
        if row not in unique_data_rows:
            unique_data_rows.append(row)

    output_data = []
    for row in unique_data_rows:
        output_row = {}
        for col, value in row.items():
            if isinstance(col, str):
                output_row[col] = value
            else:
                output_row[col.key] = value
        output_data.append(output_row)

    for input_row, output_row in zip(unique_data_rows, output_data):
        where_conditions = (
            model_att.ilike(value) if isinstance(value, str) else model_att == value
            for model_att, value in input_row.items()
            if isinstance(model_att, InstrumentedAttribute)
        )
        stmt = select(model).where(*where_conditions)
        matches = session.execute(stmt).scalars().all()

        if len(matches) > 1:
            col_to_data = (
                f'{col} - {value}'
                for col, value in zip(
                    data_col_to_model_attribute.keys(), input_row.values()
                )
            )
            rprint(
                f'The table [green]{model.__tablename__}[/] '
                'contains more than one row matching the following '
                'data, which was found in '
                f'[orange1]{data_source}[/]. Please use more '
                'columns.',
                *col_to_data,
                sep='\n',
            )
            raise Abort()

        output_row['match'] = matches[0] if matches else None

    if return_unique:
        to_return = output_data
    else:
        idxs = [unique_data_rows.index(row) for row in renamed_data]
        to_return = [output_data[i] for i in idxs]

    if not raise_error:
        return to_return

    missing = [
        {
            key: value
            for key, value in data_row.items()
            if key != 'match' and key in data_col_to_model_attribute
        }
        for data_row in output_data
        if data_row['match'] is None
    ]
    if not missing:
        return to_return

    # Checking if isinstance(col, str) for the sake of type-checking
    headers = [
        col
        for col in data[0].keys()
        if col in data_col_to_model_attribute and isinstance(col, str)
    ]
    error_table = Table(*headers)

    for values in missing:
        error_table.add_row(*values.values())

    searched_table_columns = [
        model_att.key for model_att in data_col_to_model_attribute.values()
    ]
    console = Console()
    console.print(
        f'The columns [green]{searched_table_columns}[/] in the database '
        f'table [green]{model.__tablename__}[/] were searched for rows '
        'matching the table below, which is a subset of '
        f'[orange1]{data_source}[/]. However, no matching rows '
        'were found.',
        error_table,
        sep='\n',
    )

    raise Abort()


def add_new_rows(
    session: Session,
    table: str | type[Base],
    data_col_to_model_attribute: dict[str, InstrumentedAttribute],
    data: list[dict[Hashable, Any]],
    data_source: str,
) -> None:
    """ """
    if isinstance(table, str):
        child_model = Base.get_model(table)
    elif issubclass(table, Base):
        child_model = table
    else:
        raise TypeError(
            f'Expected model to be a subclass of Base or the name of a table, but got {table}.'
        )

    child_data_col_to_model_attribute = {
        col: model_att
        for col, model_att in data_col_to_model_attribute.items()
        if model_att.parent.class_ == child_model
    }
    child_records = get_matching_rows_from_db(
        session,
        child_data_col_to_model_attribute,
        data=data,
        data_source=data_source,
        raise_error=False,
    )

    records_to_add = [
        {col: value for col, value in rec.items() if col != 'match'}
        for rec in child_records
        if rec['match'] is None
    ]
    if not records_to_add:
        return

    parent_models = {
        model_att.parent.class_
        for model_att in data_col_to_model_attribute.values()
        if model_att.parent.class_ != child_model
    }

    for parent_model in parent_models:
        parent_data_col_to_model_attributes = {
            data_col: model_att
            for data_col, model_att in data_col_to_model_attribute.items()
            if model_att.parent.class_ == parent_model
        }
        parent_records = get_matching_rows_from_db(
            session,
            parent_data_col_to_model_attributes,
            data=records_to_add,
            data_source=data_source,
            raise_error=True,
            return_unique=False,
        )
        parents = [rec['match'] for rec in parent_records]

        for child_rec, parent in zip(records_to_add, parents):
            for data_col in parent_data_col_to_model_attributes:
                child_parent_att = data_col.split('.')[0]
                child_rec[child_parent_att] = parent

    # get_matching_rows_from_db already makes sure there is just one
    # model
    model_params = [
        {att: rec.get(att) for att in child_model.__match_args__}
        for rec in records_to_add
    ]

    for param_dict, rec in zip(model_params, records_to_add):
        if None not in param_dict.values():
            session.add(child_model(**param_dict))
            continue

        none_removed_param_dict = {
            att: value for att, value in param_dict.items() if value is not None
        }
        try:
            model_to_add = child_model(**none_removed_param_dict)
        except:
            pass
        else:
            param_key_to_model_att = {
                key: getattr(child_model, key) for key in param_dict
            }
            (found,) = get_matching_rows_from_db(
                session,
                param_key_to_model_att,
                data=[param_dict],
                data_source=data_source,
                raise_error=False,
            )
            if found['match'] is None:
                session.add(model_to_add)
            continue

        available_data = rec | param_dict
        formatted_row = '\n'.join(
            f'{key}: {val}' for key, val in available_data.items()
        )

        rprint(
            f'The row shown below, taken from [orange1]{data_source}[/], will be inserted into the database table [green]{child_model.__tablename__}[/]. However, it is missing some values. This might be okay - you will be prompted if necessary.',
            formatted_row,
            sep='\n\n',
            end='\n\n',
        )

        missing_atts = [att for att, value in param_dict.items() if value is None]
        for att in missing_atts:
            model_column: InstrumentedAttribute = getattr(child_model, att)

            if getattr(model_column, '__dict__', None) is None:
                continue

            if 'nullable' not in vars(model_column):
                continue

            # TODO: make these ugly conditions and prompts cleaner
            if not model_column.nullable:
                if model_column.default is None:
                    while True:
                        param_dict[att] = Prompt.ask(
                            f'The database table [green]{child_model.__tablename__}[/] requires a value for the column [green]{att}[/], but the data source [orange1]{data_source}[/] does not contain a value for this column. Please enter a value for [green]{att}[/]',
                            default=None,
                        )
                        if param_dict[att] is not None:
                            break
                else:
                    param_dict[att] = Prompt.ask(
                        f'The database table [green]{child_model.__tablename__}[/] requires a value for the column [green]{att}[/], but the data source [orange1]{data_source}[/] does not contain a value for this column. Please enter a value for [green]{att}[/], or hit enter to use the default',
                        default=model_column.default.arg,
                    )

            else:
                param_dict[att] = Prompt.ask(
                    f'The database table [green]{child_model.__tablename__}[/] does [red]not[/] require a value for the column [green]{att}[/], and the data source [orange1]{data_source}[/] does not contain a value for this column. Please enter a value for [green]{att}[/], or press enter to leave it blank',
                    default=None,
                )
            print()

        param_dict = {
            att: value for att, value in param_dict.items() if value is not None
        }
        model_to_add = child_model(**param_dict)

        param_key_to_model_att = {key: getattr(child_model, key) for key in param_dict}
        (found,) = get_matching_rows_from_db(
            session,
            param_key_to_model_att,
            data=[param_dict],
            data_source=data_source,
            raise_error=False,
        )
        if found['match'] is None:
            session.add(model_to_add)
