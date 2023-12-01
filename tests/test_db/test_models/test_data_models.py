import pytest
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import Session, sessionmaker
from typer import Abort

from scbl_utils.db_models.data import (
    Experiment,
    Institution,
    Lab,
    Library,
    Person,
    Project,
    Sample,
    SequencingRun,
)

from ..db_fixtures import tmp_db_session


# TODO: test manual input of data
class TestInstitutionModel:
    """
    Tests for the `Institution` model.
    """

    def test_correct_ror_id(self):
        """
        Test that given a correct ROR ID, the `Institution` model
        retrieves data correctly.
        """
        data = {
            'ror_id': '02der9h97',
            'name': 'University of Connecticut',
            'short_name': 'UConn',
            'country': 'US',
            'state': 'CT',
            'city': 'Storrs',
        }

        institution = Institution(ror_id=data['ror_id'])

        for key, value in data.items():
            assert getattr(institution, key) == value

    def test_incorrect_ror_id(self):
        """
        Test that given an incorrect ROR ID, the `Institution` model
        throws an error.
        """
        with pytest.raises(Abort):
            Institution(ror_id='nonexistent_ror_id')

    def test_correct_manual_data_input(self):
        """
        Test that given manual input of data, the `Institution` model
        actually stores the correct data without manipulation.
        """
        data = {
            'ror_id': None,
            'name': 'Jackson Laboratory for Genomic Medicine',
            'short_name': 'JAX-GM',
            'country': 'US',
            'state': 'CT',
            'city': 'Farmington',
        }

        institution = Institution(**data)

        for key, value in data.items():
            assert getattr(institution, key) == value

    @pytest.mark.parametrize(argnames='missing_field', argvalues=['city', 'name'])
    def test_missing_field(
        self, tmp_db_session: sessionmaker[Session], missing_field: str
    ):
        """
        Test that given manual input of data, the `Institution` model
        throws an error if a required piece of data is missing.
        """
        data = {
            'name': 'Jackson Laboratory for Genomic Medicine',
            'short_name': 'JAX-GM',
            'state': 'CT',
            'city': 'Farmington',
        }
        data.pop(missing_field)

        with pytest.raises(StatementError):
            with tmp_db_session.begin() as session:
                session.add(Institution(**data, labs=[]))
