from pathlib import Path
from typing import Literal

import pandas as pd
from pytest import fixture, raises
from sqlalchemy import inspect, select
from sqlalchemy.orm import Session, sessionmaker
from typer import Abort

from scbl_utils.core.data_io import load_data
from scbl_utils.core.db import data_rows_to_db
from scbl_utils.db_models.base import Base
from scbl_utils.db_models.metadata_models import Institution, Lab, Person
from scbl_utils.defaults import DATA_INSERTION_ORDER, DATA_SCHEMAS

from ..fixtures.db_fixtures import (
    complete_db_objects,
    db_data,
    delivery_parent_dir,
    other_parent_names,
    table_relationships,
    tmp_db_path,
    tmp_db_session,
)


# TODO: since this function is the bulk of the main function, it would
# make sense to factor away the repetition because the tests are the
# same
def test_data_rows_to_db(
    tmp_db_session: sessionmaker[Session],
    db_data: dict[str, pd.DataFrame],
    table_relationships: dict[tuple[str, str], pd.DataFrame],
    other_parent_names: dict[str, str],
):
    for tablename in DATA_INSERTION_ORDER:
        with tmp_db_session.begin() as session:
            data_rows_to_db(
                session, data=db_data[tablename], data_source=f'test-{tablename}-data'
            )

    with tmp_db_session.begin() as session:
        for tablename in DATA_INSERTION_ORDER:
            model = Base.get_model(tablename)
            stmt = select(model)
            rows_in_db = session.execute(stmt).scalars().all()

            assert len(rows_in_db) == db_data[tablename].shape[0]

    with tmp_db_session.begin() as session:
        for (
            child_tablename,
            parent_tablename,
        ), joined_df in table_relationships.items():
            child_model = Base.get_model(child_tablename)
            stmt = select(child_model)
            children = session.execute(stmt).scalars().all()

            parent_id_col = (
                other_parent_names.get(
                    f'{child_tablename}.{parent_tablename}', parent_tablename
                )
                + '.id'
            )
            for child in children:
                assigned_parent = getattr(
                    child, parent_tablename
                )  # TODO: add a type-hint here?
                correct_parent_id = joined_df.loc[
                    joined_df[f'{child_tablename}.id'] == child.id, parent_id_col  # type: ignore
                ]

                if not isinstance(correct_parent_id, pd.Series):
                    pass
                elif len(correct_parent_id) == 1:
                    correct_parent_id = correct_parent_id.values[0]
                else:
                    raise ValueError(
                        f'Duplicate children found for {child_tablename} with id {child.id}'  # type: ignore
                    )

                if assigned_parent is None:
                    assert pd.isna(correct_parent_id)
                else:
                    assert assigned_parent.id == correct_parent_id
