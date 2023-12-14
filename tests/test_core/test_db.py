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
        # Add all the objects to the database
        with memory_db_session.begin() as session:
            session.add_all(complete_db_objects.values())

        # Iterate over each model instance and get a row from the table
        # then, feed that into matching_rows_from_table
        for model_instance in complete_db_objects.values():
            with memory_db_session.begin() as session:
                # Get the only row from this table
                stmt = select(type(model_instance))
                stored_obj: Base = session.execute(stmt).scalar()

                # Construct a dict from this object
                stored_obj_dict = {
                    key: value
                    for key, value in vars(stored_obj).items()
                    if not key.startswith('_') and not isinstance(value, Base)
                }

                # Create a dict that maps the model attributes to
                # themselves, as the keys in the above dictionary are
                # the model attributes
                att_to_data_col = {col: col for col in stored_obj_dict}

                found_rows = matching_rows_from_table(
                    session,
                    model=type(stored_obj),
                    model_attribute_to_data_col=att_to_data_col,
                    data=[stored_obj_dict],
                    data_filename='test.csv',
                )

                assert found_rows == [stored_obj]

                # Modify one of the values to make it incorrect
                key = list(stored_obj_dict.keys())[0]
                stored_obj_dict[key] = 'wrong_value'

    def test_non_matching_rows_from_table(
        self,
        memory_db_session: sessionmaker[Session],
        complete_db_objects: dict[str, Base],
    ):
        """
        Test that `matching_rows_from_table` raises an error if there
        are no matching rows.
        """
        # Add all the objects to the database
        with memory_db_session.begin() as session:
            session.add_all(complete_db_objects.values())

        # Iterate over each model instance and get a row from the table
        # then, feed that into matching_rows_from_table
        for model_instance in complete_db_objects.values():
            with memory_db_session.begin() as session:
                # Get the only row from this table
                stmt = select(type(model_instance))
                stored_obj: Base = session.execute(stmt).scalar()

                # Construct a dict from this object
                stored_obj_dict = {
                    key: value
                    for key, value in vars(stored_obj).items()
                    if not key.startswith('_') and not isinstance(value, Base)
                }

                # Modify one of the values to make it incorrect
                key = list(stored_obj_dict.keys())[0]
                stored_obj_dict[key] = 'wrong_value'

                # Create a dict that maps the model attributes to
                # themselves, as the keys in the above dictionary are
                # the model attributes
                att_to_data_col = {col: col for col in stored_obj_dict}

                with pytest.raises(Abort):
                    matching_rows_from_table(
                        session,
                        model=type(stored_obj),
                        model_attribute_to_data_col=att_to_data_col,
                        data=[stored_obj_dict],
                        data_filename='test.csv',
                    )
