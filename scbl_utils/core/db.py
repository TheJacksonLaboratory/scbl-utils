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
from dataclasses import fields
from itertools import zip_longest
from typing import Any

import pandas as pd
from numpy import rec
from rich import print as rprint
from rich.console import Console, RenderableType
from rich.prompt import Prompt
from rich.table import Table
from sqlalchemy import URL, create_engine, inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import (
    DeclarativeBase,
    InstrumentedAttribute,
    Relationship,
    Session,
    sessionmaker,
)
from typer import Abort

from scbl_utils.defaults import OBJECT_SEP_CHAR

from ..db_models import metadata_models
from ..db_models.base import Base
from ..db_models.data_models import chromium, xenium
from ..defaults import DOCUMENTATION, OBJECT_SEP_CHAR, OBJECT_SEP_PATTERN
from .utils import _print_table


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

    if not where_conditions:
        return None

    stmt = select(model).where(*where_conditions)
    matches = session.execute(stmt).scalars().all()

    if len(matches) == 0:
        return None
    elif len(matches) > 1:
        return False

    return matches[0]


# TODO: this function is good but needs some simplification. It's too
# long and should be split into smaller functions. Also, the design
# can be simplified.
def data_rows_to_db(
    session: Session, data: pd.DataFrame | list[dict[str, Any]], data_source: str
):
    """ """
    data = pd.DataFrame.from_records(data) if isinstance(data, list) else data

    child_columns = [col for col in data.columns if col.count(OBJECT_SEP_CHAR) == 1]
    model_names = {col.split(OBJECT_SEP_CHAR)[0] for col in child_columns}

    if len(model_names) == 0:
        model_names = {col.split(OBJECT_SEP_CHAR)[0] for col in data.columns}

    if len(model_names) != 1:
        rprint(
            f'The data must represent only one table in the database, but [orange1]{model_names}[/] were found.'
        )
        raise Abort()

    model_name = model_names.pop()
    model = Base.get_model(model_name)

    renamed_child_columns = [col.split(OBJECT_SEP_CHAR)[1] for col in child_columns]
    renamed_unique_data = data.drop_duplicates().rename(
        columns=dict(zip(child_columns, renamed_child_columns))
    )
    renamed_unique_child_data = renamed_unique_data[renamed_child_columns].copy()

    renamed_unique_child_data['match'] = renamed_unique_child_data.agg(
        _get_matching_obj, axis=1, session=session, model=model
    )
    data_to_add = renamed_unique_data[renamed_unique_child_data['match'].isna()]

    parent_columns = [col for col in data.columns if col.count(OBJECT_SEP_CHAR) > 1]
    parent_names = {col.split(OBJECT_SEP_CHAR)[1] for col in parent_columns}

    inspector = inspect(model)
    for parent_name in parent_names:  # TODO: model_name might be a bad name
        parent_model: type[Base] = inspector.relationships[parent_name].mapper.class_

        parent_column_pattern = (
            rf'{model_name}{OBJECT_SEP_PATTERN}{parent_name}{OBJECT_SEP_PATTERN}.*'
        )
        parent_cols = data.columns[
            data.columns.str.match(parent_column_pattern)
        ].to_list()
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

        parent_id_col = f'{parent_name}_id'
        if not inspector.columns[parent_id_col].nullable:
            no_matches = unique_parent_data[parent_name].isna()
            too_many_matches = unique_parent_data[parent_name] == False

            error_table_header = ['index'] + parent_cols
            console = Console()

            _print_table(
                unique_parent_data.loc[no_matches],
                console=console,
                header=error_table_header,
                message=f'The following rows from [orange1]{data_source}[/] could not be matched to any rows in the database table [green]{parent_model.__tablename__}[/] in assigning the [green]{parent_name}[/] for a [green]{model_name}[/]. These rows will not be added.',
            )
            _print_table(
                unique_parent_data.loc[too_many_matches],
                console=console,
                header=error_table_header,
                message=f'The following rows from [orange1]{data_source}[/] could be matched to more than one row in the database table [green]{parent_model.__tablename__}[/] in assigning the [green]{parent_name}[/] for a [green]{model_name}[/]. These rows will not be added. Please specify or add more columns in [orange1]{data_source}[/] that uniquely identify the [green]{parent_name}[/] for a [green]{model_name}[/].',
            )

            unique_parent_data = unique_parent_data[~no_matches & ~too_many_matches]
            data_to_add = data_to_add.merge(
                unique_parent_data, how='left', on=parent_cols
            ).dropna(subset=parent_name)

        else:
            unique_parent_data[parent_name] = unique_parent_data[parent_name].replace(
                False, None
            )
            data_to_add = data_to_add.merge(
                unique_parent_data, how='left', on=parent_cols
            )

    model_init_attributes = [
        field.name
        for field in fields(model)
        if field.init and field.name in data_to_add.columns
    ]

    # TODO: Is this robust? What if I miss something with 'first'
    # Write a test.
    if 'id' in data_to_add.columns:
        collection_classes = [
            (att, inspector.relationships.get(att, Relationship()).collection_class)
            for att in model_init_attributes
        ]
        agg_funcs = {
            att: 'first' if collection_class is None else collection_class
            for att, collection_class in collection_classes
        }

        grouped_records_to_add = data_to_add.groupby('id', dropna=False).agg(
            func=agg_funcs
        )
        records_to_add = grouped_records_to_add[model_init_attributes].to_dict(
            orient='records'
        )
    else:
        records_to_add = data_to_add[model_init_attributes].to_dict(orient='records')

    models_to_add = (model(**rec) for rec in records_to_add)  # type: ignore
    unique_models_to_add = []

    for model_to_add in models_to_add:
        if model_to_add not in unique_models_to_add:
            unique_models_to_add.append(model_to_add)

    session.add_all(unique_models_to_add)
