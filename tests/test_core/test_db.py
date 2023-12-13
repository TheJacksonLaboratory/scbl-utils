import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from typer import Abort

from scbl_utils.core.db import matching_rows_from_table
from scbl_utils.db_models.bases import Base

from ..fixtures.db_fixtures import (
    complete_db_objects,
    delivery_parent_dir,
    memory_db_session,
)


class TestMatchingRowsFromTable:
    """
    Tests for the `matching_rows_from_table` function.
    """

    def test_matching_rows_from_table(
        self,
        memory_db_session: sessionmaker[Session],
        complete_db_objects: dict[str, Base],
    ):
        """
        Test that `matching_rows_from_table` returns the correct rows.
        """
        with memory_db_session.begin() as session:
            session.add_all(complete_db_objects.values())

        # Iterate over each model instance and get a row from the table
        # then, feed that into matching_rows_from_table
        for model_instance in complete_db_objects.values():
            with memory_db_session.begin() as session:
                # Get the one row from this table
                stmt = select(type(model_instance))
                data_in_db = session.execute(stmt).scalar()

                # Construct the filter_dict from this row and get the
                # row using it
                filter_dict = {
                    key: value
                    for key, value in vars(data_in_db).items()
                    if not key.startswith('_') and not isinstance(value, Base)
                }
                found_rows = matching_rows_from_table(
                    session,
                    model=type(data_in_db),
                    filter_dicts=[filter_dict],
                    data_filename='test.csv',
                )

                assert found_rows == [data_in_db]

    def test_non_matching_rows_from_table(
        self,
        memory_db_session: sessionmaker[Session],
        complete_db_objects: dict[str, Base],
    ):
        """
        Test that `matching_rows_from_table` raises an error if there
        are no matching rows.
        """
        with memory_db_session.begin() as session:
            session.add_all(complete_db_objects.values())

        for model_instance in complete_db_objects.values():
            with memory_db_session.begin() as session:
                # Get the one row from this table
                stmt = select(type(model_instance))
                data_in_db = session.execute(stmt).scalar()

                # Construct the filter_dict, changing a value to make it
                # fail
                filter_dict = {
                    key: value
                    for key, value in vars(data_in_db).items()
                    if not key.startswith('_') and not isinstance(value, Base)
                }
                key = list(filter_dict.keys())[0]
                filter_dict[key] = 'wrong_value'

                with pytest.raises(Abort):
                    matching_rows_from_table(
                        session,
                        model=type(data_in_db),
                        filter_dicts=[filter_dict],
                        data_filename='test.csv',
                    )
