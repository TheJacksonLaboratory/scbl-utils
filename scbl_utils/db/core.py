from collections.abc import Mapping

import pandas as pd
from rich.console import Console
from sqlalchemy import URL, create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from .helpers import (
    construct_agg_funcs,
    date_to_id,
    get_matching_obj,
    model_from_data_columns,
    model_init_fields,
    parent_models_from_data_columns,
    required_model_init_fields,
    rich_table,
)
from .orm.base import Base
from .orm.models.data import DataSet, Platform, Sample


def assign_ids(
    datas: dict[str, pd.DataFrame], db_base_class: type[Base], session: Session
) -> Mapping[str, pd.DataFrame]:
    model_name_to_model = {
        model_name: model_from_data_columns(
            data.columns, db_model_base_class=db_base_class
        )
        for model_name, data in datas.items()
    }
    for model_name, data in datas.items():
        model = model_name_to_model[model_name]
        if issubclass(model, DataSet):
            date_col = f'{model_name}.date_initialized'
            prefix = platform.data_set_id_prefix
            id_length = platform.data_set_id_length
        elif issubclass(model, Sample):
            date_col = f'{model_name}.date_received'
            prefix = platform.sample_id_prefix
            id_length = platform.sample_id_length
        else:
            continue

        data = data.drop_duplicates(ignore_index=True)

        platform_name = data[f'{model_name}.platform.name'].iloc[0]
        platform = session.execute(
            select(Platform).where(Platform.name.ilike(platform_name))
        ).scalar()

        if platform is None:
            raise ValueError(f'Platform [orange1]{platform_name}[/] does not exist')

        data[f'{model_name}.id'] = data[[f'{model_name}.{date_col}']].apply(
            date_to_id, axis=1, prefix=prefix, id_length=id_length
        )

        datas[model_name] = data.copy()

    # TODO: can we make this more elegant
    for model_name, data in datas.items():
        model = model_name_to_model[model_name]
        if 'data_set' not in model_init_fields(model):
            continue

        data_set_parent_name = next(
            parent_model_name
            for parent_model_name, parent_model in parent_models_from_data_columns(
                data.columns, model=model
            ).items()
            if issubclass(parent_model, DataSet)
        )

        parent_columns_on_child = data.columns[
            data.columns.str.match(rf'{model_name}\.data_set\.')
        ]
        parent_columns_on_parent = datas[data_set_parent_name].columns[
            data.columns.str.fullmatch(rf'{data_set_parent_name}\.\w+')
        ]

        data = data.merge(
            datas[data_set_parent_name],
            how='left',
            left_on=parent_columns_on_child,
            right_on=parent_columns_on_parent,
        )

        data[f'{model_name}.{data_set_parent_name}.id'] = data[
            f'{data_set_parent_name}.id'
        ]
        datas[model_name] = data.drop(columns=parent_columns_on_parent)

    return datas


def db_session(db_base_class: type[Base], **kwargs) -> sessionmaker[Session]:
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
    db_base_class.metadata.create_all(engine)

    return Session


# TODO: this function is good but needs some simplification. It's too
# long and should be split into smaller functions. Also, the design
# can be simplified.
def data_rows_to_db(
    db_base_class: type[Base],
    session: Session,
    data: pd.DataFrame,
    data_source: str,
    console: Console,
):
    """"""
    model = model_from_data_columns(data.columns, db_model_base_class=db_base_class)
    parent_models = parent_models_from_data_columns(data.columns, model=model)

    column_renamer = {col: col.split('.', maxsplit=1)[1] for col in data.columns}
    data = data.rename(columns=column_renamer).drop_duplicates()

    exist_in_db = data.agg(get_matching_obj, axis=1, session=session, model=model)
    new_data = data[exist_in_db.isna()]

    if new_data.empty:
        return

    required_fields = required_model_init_fields(model)
    for (
        parent_name,
        parent_model,
    ) in parent_models.items():  # TODO: parent_name might be a bad name
        parent_columns = new_data.columns[
            new_data.columns.str.startswith(parent_name)
        ].to_list()
        parent_data = new_data.drop_duplicates(subset=parent_columns).copy()

        parent_data[parent_name] = parent_data.agg(
            get_matching_obj, axis=1, session=session, model=parent_model
        )

        if parent_name in required_fields:
            no_matches = parent_data[parent_name].isna()
            too_many_matches = parent_data[parent_name] == False

            error_table_header = ['index'] + parent_columns

            if no_matches.any():
                console.print(
                    f'The following rows from {data_source} could not be matched to any rows in the database table [green]{parent_model.__tablename__}[/] in assigning the [green]{parent_name}[/] for a [green]{model.__name__}[/]. These rows will not be added.'
                )
                no_matches_table = rich_table(
                    parent_data.loc[no_matches, parent_columns],
                    header=error_table_header,
                )
                console.print(no_matches_table, end='\n\n')

            if too_many_matches.any():
                console.print(
                    f'The following rows from {data_source} were matched to more than one row in the database table [green]{parent_model.__tablename__}[/] in assigning the [green]{parent_name}[/] for a [green]{model.__name__}[/]. These rows will not be added. Please specify or add more columns in {data_source} that uniquely identify the [green]{parent_name} for a [green]{model.__name__}[/].'
                )
                too_many_matches_table = rich_table(
                    parent_data.loc[too_many_matches, parent_columns],
                    header=error_table_header,
                )
                console.print(too_many_matches_table, end='\n\n')

            parent_data = parent_data[~no_matches & ~too_many_matches]
            new_data = new_data.merge(parent_data, how='left', on=parent_columns)

        else:
            parent_data[parent_name] = parent_data[parent_name].replace(False, None)
            new_data = new_data.merge(parent_data, how='left', on=parent_columns)

    if 'id' in new_data.columns:
        agg_funcs = construct_agg_funcs(model, data_columns=new_data.columns)
        new_data = new_data.groupby('id').agg(func=agg_funcs)

    new_data = new_data[model_init_fields(model)].dropna(
        subset=required_fields, how='any'
    )
    records = new_data.to_dict(orient='records')

    model_instances = []
    for rec in records:
        try:
            model_instances.append(model(**rec))  # type: ignore
        except Exception as e:
            console.print(f'{rec} could not be added: {e}', end='\n\n')

    stmt = select(model)
    existing_models = session.execute(stmt).scalars().all()

    unique_new_instances = []
    for instance in model_instances:
        if instance not in unique_new_instances and instance not in existing_models:
            unique_new_instances.append(instance)
            try:
                session.add(instance)
            except Exception as e:
                console.print(f'{instance} could not be added: {e}', end='\n\n')
