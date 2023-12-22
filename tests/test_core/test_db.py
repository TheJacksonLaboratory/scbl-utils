from pytest import raises
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from typer import Abort

from scbl_utils.core.db import get_matching_rows_from_db
from scbl_utils.db_models.bases import Base
from scbl_utils.db_models.data import Institution

from ..fixtures.db_fixtures import (
    complete_db_objects,
    db_path,
    delivery_parent_dir,
    test_db_session,
)


class TestMatchingRowsFromTable:
    """
    Tests for the `matching_rows_from_table` function.
    """

    def test_matching_rows_from_table(
        self,
        test_db_session: sessionmaker[Session],
        complete_db_objects: dict[str, Base],
    ):
        """
        Test that `matching_rows_from_table` returns the correct rows.
        This doesn't test every table, which may be a future TODO.
        """
        # Add the institution to the database
        with test_db_session.begin() as session:
            session.add(complete_db_objects['institution'])

        # Test that the function returns the correct (and only) institution
        with test_db_session.begin() as session:
            matched_institutions = get_matching_rows_from_db(
                session,
                {Institution.short_name: 'institution_short_name'},
                data=[{'institution_short_name': 'JAX-GM'}],
                data_filename='test.csv',
            )
            db_institution = session.execute(select(Institution)).scalar()

            assert matched_institutions[0] == db_institution

        # Test that given wrong information, the function raises
        with test_db_session.begin() as session:
            with raises(Abort):
                get_matching_rows_from_db(
                    session,
                    {Institution.short_name: 'institution_short_name'},
                    data=[{'institution_short_name': 'wrong_value'}],
                    data_filename='test.csv',
                )
