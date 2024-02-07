from numpy import nan
from sqlalchemy.orm import DeclarativeBase, sessionmaker, Session, Mapper, Relationship
from sqlalchemy import URL, create_engine, inspect, select
from .orm.base import Base
from typing import Any
import pandas as pd
from dataclasses import fields, MISSING
from logging import Logger
from .helpers import rich_table
from rich.console import Console
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

def construct_where_condition(
    attribute_name: str, value: Any, model_inspector: Mapper[Base]
):
    if '.' not in attribute_name:
        attribute = model_inspector.attrs[attribute_name].class_attribute
        return attribute.ilike(value) if isinstance(value, str) else attribute == value

    parent_name, parent_attribute_name = attribute_name.split(
        '.', maxsplit=1
    )
    parent_inspector = model_inspector.relationships[parent_name].mapper
    parent = model_inspector.attrs[parent_name].class_attribute

    parent_where_condition = construct_where_condition(
        parent_attribute_name, value, model_inspector=parent_inspector
    )
    return parent.has(parent_where_condition)


def get_matching_obj(
    data: pd.Series, session: Session, model: type[Base]
) -> Base | None | bool:
    where_conditions = []

    for col, val in data.items():
        if not isinstance(col, str):
            continue

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
    session: Session, data: pd.DataFrame | list[dict[str, Any]], data_source: str, log: Logger
):
    """ """
    data = pd.DataFrame.from_records(data) if isinstance(data, list) else data
    data = data.drop_duplicates()

    model_names = {col.split('.')[0] for col in data.columns}

    if len(model_names) != 1:
        raise ValueError(f'The data must represent only one table in the database, but {model_names} were found')

    model_name = model_names.pop()
    model: type[Base] = next(mapper.class_ for mapper in Base.registry.mappers if mapper.class_.__name__ == model_name)

    model_init_fields = {field.name: field for field in fields(model) if field.init}
    required_model_init_fields = {
        field_name: field
        for field_name, field in model_init_fields.items()
        if field.default is MISSING and field.default_factory is MISSING
    }
    renamed_data_columns = {col.split('.')[1] for col in data.columns}

    missing_fields = ', '.join(required_model_init_fields.keys() - renamed_data_columns)
    if missing_fields:
        raise ValueError(f'The following fields are required to initialize a {model_name}, but are missing from the columns of {data_source}: {missing_fields}')

    column_renamer = {
        col: col.split('.', maxsplit=1)[1] for col in data.columns
    }
    renamed_unique_data = data.drop_duplicates().rename(columns=column_renamer)
    renamed_unique_data['match'] = renamed_unique_data.agg(
        get_matching_obj, axis=1, session=session, model=model
    )
    data_to_add = renamed_unique_data[renamed_unique_data['match'].isna()]

    if data_to_add.empty:
        return

    parent_columns = [col for col in data.columns if col.count('.') > 1]
    parent_names = {col.split('.')[1] for col in parent_columns}

    inspector = inspect(model)
    for parent_name in parent_names:  # TODO: parent_name might be a bad name
        parent_model: type[Base] = inspector.relationships[parent_name].mapper.class_

        parent_column_pattern = rf'{parent_name}\..*'
        parent_columns = data_to_add.columns[
            data_to_add.columns.str.match(parent_column_pattern)
        ].to_list()
        unique_parent_data = data_to_add[parent_columns].drop_duplicates()

        renamed_parent_columns = [
            col.split('.', maxsplit=1)[1] for col in parent_columns
        ]
        parent_column_renamer = dict(zip(parent_columns, renamed_parent_columns))
        renamed_unique_parent_data = unique_parent_data.rename(
            columns=parent_column_renamer
        )

        unique_parent_data[parent_name] = renamed_unique_parent_data.agg(
            get_matching_obj, axis=1, session=session, model=parent_model
        )

        if parent_name in required_model_init_fields:
            no_matches = unique_parent_data[parent_name].isna()
            too_many_matches = unique_parent_data[parent_name] == False

            error_table_header = ['index'] + parent_columns

            console = Console()
            if no_matches.any():
                no_matches_table = rich_table(unique_parent_data.loc[no_matches], header=error_table_header)
                log.warning(f'The following rows from {data_source} could not be matched to any rows in the database table [green]{parent_model.__tablename__}[/] in assigning the [green]{parent_name}[/] for a [green]{model_name}[/]. These rows will not be added.')
                console.print(no_matches_table)

            if too_many_matches.any():
                too_many_matches_table = rich_table(unique_parent_data.loc[too_many_matches], header=error_table_header)
                log.warning(f'The following rows from {data_source} were matched to more than one row in the database table [green]{parent_model.__tablename__}[/] in assigning the [green]{parent_name}[/] for a [green]{model_name}[/]. These rows will not be added. Please specify or add more columns in {data_source} that uniquely identify the [green]{parent_name} for a [green]{model_name}[/].')
                console.print(too_many_matches_table)

            unique_parent_data = unique_parent_data[~no_matches & ~too_many_matches]
            data_to_add = data_to_add.merge(
                unique_parent_data, how='left', on=parent_columns
            ).dropna(subset=parent_name)

        else:
            unique_parent_data[parent_name] = unique_parent_data[parent_name].replace(
                False, nan
            )
            data_to_add = data_to_add.merge(
                unique_parent_data, how='left', on=parent_columns
            )

    model_init_fields_in_data = {
        field_name: field
        for field_name, field in model_init_fields.items()
        if field_name in data_to_add.columns
    }

    # TODO: Is this robust? What if I miss something with 'first'
    # Write a test.
    if 'id' in data_to_add.columns:
        collection_classes = [
            (att, inspector.relationships.get(att, Relationship()).collection_class)
            for att in model_init_fields_in_data
        ]
        agg_funcs = {
            att: 'first' if collection_class is None else collection_class
            for att, collection_class in collection_classes
        }

        grouped_data_to_add = data_to_add.groupby('id', dropna=False).agg(
            func=agg_funcs
        )
        data_to_add = grouped_data_to_add[model_init_fields_in_data.keys()]
    else:
        data_to_add = data_to_add[model_init_fields_in_data.keys()]

    records_to_add = data_to_add.dropna(
        subset=list(required_model_init_fields), how='any'
    ).to_dict(orient='records')

    models_to_add = []
    for rec in records_to_add:
        try:
            models_to_add.append(model(**rec)) # type: ignore
        except:
            log.warning(f'The following record from {data_source} will not be added to the database because it is invalid: {rec}', exc_info=True)

    stmt = select(model)
    existing_models = session.execute(stmt).scalars().all()

    unique_new_models = []
    for model_to_add in models_to_add:
        if (
            model_to_add not in unique_new_models
            and model_to_add not in existing_models
        ):
            unique_new_models.append(model_to_add)
    session.add_all(unique_new_models)
