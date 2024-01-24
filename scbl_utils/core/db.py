"""
This module contains functions related to database operations used in
`main.py` to create a command-line interface.

Functions:
    - `db_session`: Create and return a new database session,
    populating with tables if necessary

    - `matching_rows_from_table`: Get rows from a table that match
    certain criteria
"""
from collections.abc import Container, Hashable, Sequence
from itertools import zip_longest
from typing import Any

import pandas as pd
from numpy import rec
from rich import print as rprint
from rich.console import Console, RenderableType
from rich.prompt import Prompt
from rich.table import Table
from sqlalchemy import URL, create_engine, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import (
    DeclarativeBase,
    InstrumentedAttribute,
    Relationship,
    Session,
    sessionmaker,
)
from typer import Abort

from ..db_models.bases import Base
from ..defaults import DOCUMENTATION, OBJECT_SEP_CHAR, OBJECT_SEP_PATTERN
from .utils import _get_matching_obj, _print_table


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


# TODO: this function is good but needs some simplification. Come back to it
def data_rows_to_db(
    session: Session, data: pd.DataFrame | list[dict[str, Any]], data_source: str
):
    """ """
    data = pd.DataFrame.from_records(data) if isinstance(data, list) else data

    inherent_attribute_cols = [
        col for col in data.columns if col.count(OBJECT_SEP_CHAR) == 1
    ]
    tables = {col.split(OBJECT_SEP_CHAR)[0] for col in inherent_attribute_cols}

    if len(tables) == 0:
        tables = {col.split(OBJECT_SEP_CHAR)[0] for col in data.columns}

    if len(tables) != 1:
        rprint(
            f'The data must represent only one table in the database, but [orange1]{tables}[/] were found.'
        )
        raise Abort()

    table = tables.pop()
    model = Base.get_model(table)

    renamed_inherent_attribute_cols = [
        col.split(OBJECT_SEP_CHAR)[1] for col in inherent_attribute_cols
    ]
    renamed_unique_data = data.drop_duplicates().rename(
        columns=dict(zip(inherent_attribute_cols, renamed_inherent_attribute_cols))
    )
    renamed_unique_inherent_data = renamed_unique_data[
        renamed_inherent_attribute_cols
    ].copy()

    renamed_unique_inherent_data['match'] = renamed_unique_inherent_data.agg(
        _get_matching_obj, axis=1, session=session, model=model
    )
    data_to_add = renamed_unique_data[renamed_unique_inherent_data['match'].isna()]

    parent_attribute_cols = [
        col for col in data.columns if col.count(OBJECT_SEP_CHAR) > 1
    ]
    parent_names = {col.split(OBJECT_SEP_CHAR)[1] for col in parent_attribute_cols}

    inspector = inspect(model)
    for parent_name in parent_names:  # TODO: model_name might be a bad name
        parent_model: type[Base] = inspector.relationships[parent_name].mapper.class_

        parent_col_pattern = (
            rf'{table}{OBJECT_SEP_PATTERN}{parent_name}{OBJECT_SEP_PATTERN}.*'
        )
        parent_cols = data.columns[data.columns.str.match(parent_col_pattern)].to_list()
        unique_parent_data = data[parent_cols].drop_duplicates()

        renamed_cols = [
            col.split(OBJECT_SEP_CHAR, maxsplit=2)[2] for col in parent_cols
        ]
        renamed_unique_parent_data = unique_parent_data.rename(
            columns=dict(zip(parent_cols, renamed_cols))
        )

        unique_parent_data[parent_name] = renamed_unique_parent_data.agg(
            _get_matching_obj, axis=1, session=session, model=parent_model
        )

        no_matches = unique_parent_data[parent_name].isna()
        too_many_matches = ~unique_parent_data[parent_name]

        error_table_header = ['index'] + parent_cols
        console = Console()

        _print_table(
            unique_parent_data.loc[no_matches],
            console=console,
            header=error_table_header,
            message=f'The following rows from [orange1]{data_source}[/] could be matched to any rows in the database table [green]{parent_model.__tablename__}[/] in assigning the [green]{parent_name}[/] for a [green]{table}[/]. These rows will not be added.',
        )
        _print_table(
            unique_parent_data.loc[too_many_matches],
            console=console,
            header=error_table_header,
            message=f'The following rows from [orange1]{data_source}[/] could be matched to more than one row in the database table [green]{parent_model.__tablename__}[/] in assigning the [green]{parent_name}[/] for a [green]{table}[/]. These rows will not be added. Please specify or add more columns in [orange1]{data_source}[/] that uniquely identify the [green]{parent_name}[/] for a [green]{table}[/].',
        )

        data_to_add = data_to_add[~no_matches & ~too_many_matches]
        data_to_add = data_to_add.merge(unique_parent_data, how='left', on=parent_cols)

    model_attributes = data_to_add.columns[
        data_to_add.columns.isin(model.__match_args__)
    ]

    # TODO: Is this robust? What if I miss something with 'first'
    if 'id' in data_to_add.columns:
        collection_class = {
            att: inspector.relationships.get(att, Relationship()).collection_class
            for att in model_attributes
        }
        agg_funcs = {
            att: 'first' if collection_class[att] is None else collection_class[att]
            for att in model_attributes
        }
        records_to_add = (
            data_to_add[model_attributes]
            .groupby(
                'id', dropna=False
            )  # TODO: this means that anything with a missing ID will be grouped together. Test this
            .agg(func=agg_funcs)
            .to_dict(orient='records')
        )
    else:
        records_to_add = data_to_add[model_attributes].to_dict(orient='records')

    models_to_add = (model(**rec) for rec in records_to_add)  # type: ignore
    unique_models_to_add = []

    for model_to_add in models_to_add:
        if model_to_add not in unique_models_to_add:
            unique_models_to_add.append(model_to_add)

    session.add_all(unique_models_to_add)
