from pathlib import Path
from string import punctuation, whitespace

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from typer import Abort

from scbl_utils.db_models.data import Institution, Lab, Library, Person, Project, Sample

from ..db_fixtures import memory_db_session, delivery_parent_dir, full_db


class TestInstitutionModel:
    """
    Tests for the `Institution` model.
    """

    ror_id = '02der9h97'
    manual_name = 'A manually assigned name'
    correct_dataset = [
        (
            {'ror_id': whitespace + ror_id + whitespace},
            {
                'ror_id': ror_id,
                'name': 'University of Connecticut',
                'short_name': 'UConn',
                'country': 'US',
                'state': 'CT',
                'city': 'Storrs',
            },
        ),
        (
            {
                'ror_id': whitespace + ror_id + whitespace,
                'name': whitespace + manual_name + whitespace,
                'short_name': whitespace + manual_name + whitespace,
                'country': 'wrong_country',
                'state': 'wrong_state',
                'city': 'wrong_city',
            },
            {
                'ror_id': ror_id,
                'name': manual_name,
                'short_name': manual_name,
                'country': 'US',
                'state': 'CT',
                'city': 'Storrs',
            },
        ),
    ]

    @pytest.mark.parametrize(
        argnames=['institution_data', 'expected_institution'], argvalues=correct_dataset
    )
    def test_correct_ror_id(
        self,
        institution_data: dict[str, str],
        expected_institution: dict[str, str],
        memory_db_session: sessionmaker[Session],
    ):
        """
        Test that given a correct ROR ID, the `Institution` model
        retrieves data correctly. Also tests string stripping. Note
        that this is the only time that `StrippedString` is tested,
        as all other models use the same `StrippedString` type.
        """
        institution = Institution(**institution_data, labs=[])

        with memory_db_session.begin() as session:
            session.add(institution)

        with memory_db_session.begin() as session:
            stmt = select(Institution)
            processed_institution = session.execute(stmt).scalar()

            for key, value in expected_institution.items():
                assert getattr(processed_institution, key) == value

    def test_incorrect_ror_id(self):
        """
        Test that given an incorrect ROR ID, the `Institution` model
        throws an error.
        """
        with pytest.raises(Abort):
            Institution(ror_id='nonexistent_ror_id')


class TestLabModel:
    """
    Tests for the `Lab` model.
    """

    def test_auto_setting(self, full_db: dict, delivery_parent_dir: Path):
        """
        Test that the `Lab` model correctly sets the `delivery_dir` and
        `name` attributes when given the minimum required information.
        """
        # Get the two necessary objects for a Lab
        institution: Institution = full_db['institution']
        pi: Person = full_db['person']

        # Define the expected lab and create the Lab object. Note that
        # the group is hardcoded to 'test_group' because of the fixture
        # delivery_parent_dir, which is called by full_db
        expected_lab = {
            'delivery_dir': str(
                delivery_parent_dir / f'{pi.first_name.lower()}_{pi.last_name.lower()}'
            ),
            'group': 'test_group',
            'name': 'Said Lab',
            'projects': [],
        }
        lab = Lab(institution=institution, pi=pi)

        for key, value in expected_lab.items():
            assert getattr(lab, key) == value


class TestProjectModel:
    """
    Tests for the `Project` model.
    """

    def test_invalid_project_id(self, full_db: dict):
        """
        Test that the `Project` model raises error with invalid project ID.
        """
        with pytest.raises(Abort):
            Project(id='fake-id', lab=full_db['lab'])


class TestPersonModel:
    """
    Tests for the `Person` model.
    """

    def test_valid_orcid(self):
        """
        Test that the `Person` model accepts the ORCID, regardless of
        the number of dashes.
        """
        orcid = '0009-0008-3754-6150'
        n_dashes = orcid.count('-')
        people = [
            Person(
                first_name='Ahmed', last_name='Said', orcid=orcid.replace('-', '', i)
            )
            for i in range(1, n_dashes + 1)
        ]
        assert all(person.orcid == orcid for person in people)

    @pytest.mark.parametrize(
        argnames=[
            'orcid',
        ],
        argvalues=[
            ('fake-orcid',),
            ('9999-9999-9999-9999',),
        ],
    )
    def test_invalid_orcid(self, orcid: str):
        """
        Test that the `Person` model raises error with invalid ORCID.
        """
        with pytest.raises(Abort):
            Person(first_name='Ahmed', last_name='Said', orcid=orcid)


class TestExperimentModel:
    """
    Tests for the `Experiment` model.
    """

    pass


class TestSampleModel:
    """
    Tests for the `Sample` model.
    """

    def test_sample_name(self, full_db: dict, memory_db_session: sessionmaker[Session]):
        """
        Test that the `Sample` model correctly cleans the sample name.
        """

        # Get the necessary object for a sample
        experiment = full_db['experiment']

        illegal_punctuation = punctuation.replace('_', '').replace('-', '')
        sample_name = f'{illegal_punctuation}some{whitespace}name{illegal_punctuation}'
        sample = Sample(sample_name, experiment=experiment)

        with memory_db_session.begin() as session:
            session.add(sample)

        with memory_db_session.begin() as session:
            stmt = select(Sample)
            processed_sample: Sample = session.execute(stmt).scalar()
            assert processed_sample.name == 'some-name'


class TestSequencingRunModel:
    """
    Tests for the `SequencingRun` model.
    """

    pass


class TestLibraryModel:
    """
    Tests for the `Library` model.
    """

    @pytest.mark.parametrize(
        argnames=['library_id', 'expected_library_id'],
        argvalues=[(f'{whitespace}sc9900000{whitespace}', 'SC9900000')],
    )
    def test_valid_library_id(
        self, full_db: dict, library_id: str, expected_library_id: str
    ):
        """
        Test that the `Library` model cleans the library ID.
        """
        experiment = full_db['experiment']
        library_type = full_db['library_type']
        library = Library(
            id=library_id, experiment=experiment, library_type=library_type
        )
        assert library.id == expected_library_id

    def test_invalid_library_id(self, full_db: dict):
        """
        Test that the `Library` model raises error with invalid library ID.
        """
        experiment = full_db['experiment']
        library_type = full_db['library_type']
        with pytest.raises(Abort):
            experiment = full_db['experiment']
            Library(id='fake-id', experiment=experiment, library_type=library_type)
