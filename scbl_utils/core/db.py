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

from ..db_models.bases import Base
from ..defaults import DOCUMENTATION, OBJECT_SEP_CHAR, OBJECT_SEP_PATTERN
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


def get_matching_obj(
    data: pd.Series, session: Session, model: type[Base]
) -> Base | None:
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
    match = session.execute(stmt).scalar()

    return match


# TODO: this function is good but needs some simplification. Come back to it
def add_dependent_rows(
    session: Session, data: pd.DataFrame | list[dict[str, Any]], data_source: str
):
    """ """
    data = pd.DataFrame.from_records(data) if isinstance(data, list) else data

    # TODO: again, some kind of validation about the format of column names will have to happen, whether using jsonschema or pydantic
    inherent_attribute_cols = [
        col for col in data.columns if col.count(OBJECT_SEP_CHAR) == 1
    ]
    tables = {col.split(OBJECT_SEP_CHAR)[0] for col in inherent_attribute_cols}

    if len(tables) == 0:
        tables = {col.split(OBJECT_SEP_CHAR)[0] for col in data.columns}

    # TODO: this validation can be taken elsewhere. Probably in the CSV schema. also improve the error message here
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
        get_matching_obj, axis=1, session=session, model=model
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
            get_matching_obj, axis=1, session=session, model=parent_model
        )
        no_matches = unique_parent_data[parent_name].isna()

        error_table_header = ['index'] + parent_cols
        error_table = Table(*error_table_header)

        for idx, parent_row in unique_parent_data.loc[no_matches].iterrows():
            # Filter and str() for type-checking
            error_table.add_row(str(idx), *(str(v) for v in parent_row.values))

        if error_table.row_count > 0:
            console = Console()
            console.print(
                f'The following rows from [orange1]{data_source}[/] could be matched to any rows in the database table [green]{parent_model.__tablename__}[/] in assigning the [green]{parent_name}[/] for a [green]{table}[/]. These rows will not be added.',
                error_table,
                sep='\n',
            )

        data_to_add = data_to_add.merge(
            unique_parent_data, how='left', on=parent_cols
        ).dropna(subset=parent_name)

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

    # try:
    for model_to_add in models_to_add:
        if model_to_add not in unique_models_to_add:
            unique_models_to_add.append(model_to_add)
    # except:
    #     print(model)
    #     print(*records_to_add, sep='\n')
    #     print(renamed_unique_data)
    #     quit()

    session.add_all(unique_models_to_add)
