from pathlib import Path
from typing import Literal

import pandas as pd
from pytest import fixture, raises
from sqlalchemy import inspect, select
from sqlalchemy.orm import Session, sessionmaker
from typer import Abort

from scbl_utils.core.data_io import load_data
from scbl_utils.core.db import data_rows_to_db
from scbl_utils.db_models.bases import Base
from scbl_utils.db_models.data import Institution, Lab, Person
from scbl_utils.defaults import DATA_INSERTION_ORDER, DATA_SCHEMAS

from ..fixtures.db_fixtures import (
    complete_db_objects,
    db_data,
    db_path,
    delivery_parent_dir,
    other_parent_names,
    table_relationships,
    test_db_session,
)


# TODO: since this function is the bulk of the main function, it would
# make sense to factor away the repetition because the tests are the
# same
def test_data_rows_to_db(
    test_db_session: sessionmaker[Session],
    db_data: dict[str, pd.DataFrame],
    table_relationships: dict[tuple[str, str], pd.DataFrame],
    other_parent_names: dict[str, str],
):
    for tablename in DATA_INSERTION_ORDER:
        with test_db_session.begin() as session:
            data_rows_to_db(
                session, data=db_data[tablename], data_source=f'test-{tablename}-data'
            )

    with test_db_session.begin() as session:
        for tablename in DATA_INSERTION_ORDER:
            model = Base.get_model(tablename)
            stmt = select(model)
            rows_in_db = session.execute(stmt).scalars().all()

            assert len(rows_in_db) == db_data[tablename].shape[0]

    with test_db_session.begin() as session:
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
                    joined_df[f'{child_tablename}.id'] == child.id, parent_id_col
                ]

                if not isinstance(correct_parent_id, pd.Series):
                    pass
                elif len(correct_parent_id) == 1:
                    correct_parent_id = correct_parent_id.values[0]
                else:
                    raise ValueError(
                        f'Duplicate children found for {child_tablename} with id {child.id}'
                    )

                if assigned_parent is None:
                    assert pd.isna(correct_parent_id)
                else:
                    assert assigned_parent.id == correct_parent_id


# class TestMatchingRowsFromTable:
#     """
#     Tests for the `matching_rows_from_table` function.
#     """

#     def test_matching_rows_from_table(
#         self,
#         test_db_session: sessionmaker[Session],
#         complete_db_objects: dict[str, Base],
#     ):
#         """
#         Test that `matching_rows_from_table` returns the correct rows.
#         This doesn't test every table, which may be a future TODO.
#         """
#         # Add the institution to the database
#         with test_db_session.begin() as session:
#             session.add(complete_db_objects['institution'])

#         # Test that the function returns the correct (and only) institution
#         with test_db_session.begin() as session:
#             matched_institutions = get_matching_rows(
#                 session,
#                 {Institution.short_name: 'institution_short_name'},
#                 data=[{'institution_short_name': 'JAX-GM'}],
#                 data_filename='test.csv',
#             )
#             db_institution = session.execute(select(Institution)).scalar()

#             assert matched_institutions[0] == db_institution

#         # Test that given wrong information, the function raises
#         with test_db_session.begin() as session:
#             with raises(Abort):
#                 get_matching_rows(
#                     session,
#                     {Institution.short_name: 'institution_short_name'},
#                     data=[{'institution_short_name': 'wrong_value'}],
#                     data_filename='test.csv',
#                 )
