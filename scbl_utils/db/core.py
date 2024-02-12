from dataclasses import fields
from typing import Any

import pandas as pd
from rich.console import Console
from sqlalchemy import URL, create_engine, inspect, select
from sqlalchemy.orm import DeclarativeBase, Relationship, Session, sessionmaker

from ..validation import validate_data_columns
from .helpers import (
    child_model_from_data_columns,
    construct_agg_funcs,
    construct_where_condition,
    model_init_fields,
    parent_models_from_data_columns,
    required_model_init_fields,
    rich_table,
)
from .orm.base import Base, Model


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
    data: pd.Series | dict, session: Session, model: type[Model]
) -> Model | None | bool:
    where_conditions = []

    model_field_names = (field.name for field in fields(model))
    cleaned_data = {
        col: val
        for col, val in data.items()
        if not pd.isna(val) and isinstance(col, str) and col in model_field_names
    }
    for col, val in cleaned_data.items():
        inspector = inspect(model)
        where = construct_where_condition(col, value=val, model_inspector=inspector)
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
    session: Session,
    data: pd.DataFrame | list[dict[str, Any]],
    data_source: str,
    console: Console,
):
    """"""
    validate_data_columns(
        data.columns, db_model_base_class=Base, data_source=data_source
    ) if isinstance(data, pd.DataFrame) else validate_data_columns(
        data[0].keys(), db_model_base_class=Base, data_source=data_source
    )

    data = pd.DataFrame.from_records(data).drop_duplicates()
    child_model = child_model_from_data_columns(data.columns, db_model_base_class=Base)

    column_renamer = {col: col.split('.', maxsplit=1)[1] for col in data.columns}
    renamed_data = data.rename(columns=column_renamer)

    matches = renamed_data.agg(
        get_matching_obj, axis=1, session=session, model=child_model
    )
    data_to_add = renamed_data[matches.isna()]

    if data_to_add.empty:
        return

    parent_models = parent_models_from_data_columns(
        data.columns, child_model=child_model
    )
    required_fields = required_model_init_fields(child_model)
    for (
        parent_name,
        parent_model,
    ) in parent_models.items():  # TODO: parent_name might be a bad name
        parent_columns = data_to_add.columns[
            data_to_add.columns.str.startswith(parent_name)
        ].to_list()
        unique_parent_data = data_to_add[parent_columns].drop_duplicates()

        unique_parent_data[parent_name] = unique_parent_data.agg(
            get_matching_obj, axis=1, session=session, model=parent_model
        )

        if parent_name in required_model_init_fields(parent_model):
            no_matches = unique_parent_data[parent_name].isna()
            too_many_matches = unique_parent_data[parent_name] == False

            error_table_header = ['index'] + parent_columns

            if no_matches.any():
                console.print(
                    f'The following rows from {data_source} could not be matched to any rows in the database table [green]{parent_model.__tablename__}[/] in assigning the [green]{parent_name}[/] for a [green]{child_model.__name__}[/]. These rows will not be added.'
                )
                no_matches_table = rich_table(
                    unique_parent_data.loc[no_matches], header=error_table_header
                )
                console.print(no_matches_table, end='\n\n')

            if too_many_matches.any():
                console.print(
                    f'The following rows from {data_source} were matched to more than one row in the database table [green]{parent_model.__tablename__}[/] in assigning the [green]{parent_name}[/] for a [green]{child_model.__name__}[/]. These rows will not be added. Please specify or add more columns in {data_source} that uniquely identify the [green]{parent_name} for a [green]{child_model.__name__}[/].'
                )
                too_many_matches_table = rich_table(
                    unique_parent_data.loc[too_many_matches], header=error_table_header
                )
                console.print(too_many_matches_table, end='\n\n')

            unique_parent_data = unique_parent_data[~no_matches & ~too_many_matches]
            data_to_add = data_to_add.merge(
                unique_parent_data, how='left', on=parent_columns
            )

        else:
            unique_parent_data[parent_name] = unique_parent_data[parent_name].replace(
                False, None
            )
            data_to_add = data_to_add.merge(
                unique_parent_data, how='left', on=parent_columns
            )

    model_init_fields_in_data = [
        col for col in model_init_fields(child_model) if col in data_to_add.columns
    ]
    # TODO: Is this robust? What if I miss something with 'first'
    # Write a test.
    if 'id' in data_to_add.columns:
        agg_funcs = construct_agg_funcs(child_model, data_columns=data_to_add.columns)

        grouped_data_to_add = data_to_add.groupby('id', dropna=False).agg(
            func=agg_funcs
        )
        data_to_add = grouped_data_to_add[model_init_fields_in_data]
    else:
        data_to_add = data_to_add[model_init_fields_in_data]

    records_to_add = data_to_add.dropna(
        subset=list(required_fields.keys()), how='any'
    ).to_dict(orient='records')

    models_to_add = []
    for rec in records_to_add:
        try:
            models_to_add.append(child_model(**rec))  # type: ignore
        except Exception as e:
            console.print(str(e), end='\n\n')

    stmt = select(child_model)
    existing_models = session.execute(stmt).scalars().all()

    unique_new_models = []
    for model_to_add in models_to_add:
        if (
            model_to_add not in unique_new_models
            and model_to_add not in existing_models
        ):
            unique_new_models.append(model_to_add)
            try:
                session.add(model_to_add)
            except Exception as e:
                console.print(str(e), end='\n\n')
