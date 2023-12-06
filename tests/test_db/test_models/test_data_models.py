from pathlib import Path

import pytest
from typer import Abort

from scbl_utils.db_models.data import Institution, Lab, Library, Person, Project

from ..db_fixtures import full_db


class TestInstitutionModel:
    """
    Tests for the `Institution` model.
    """

    ror_id = '02der9h97'
    correct_dataset = [
        (
            {'ror_id': ror_id},
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
                'ror_id': ror_id,
                'name': 'A manually assigned name',
                'short_name': 'A manually assigned short name',
                'country': 'wrong_country',
                'state': 'wrong_state',
                'city': 'wrong_city',
            },
            {
                'ror_id': ror_id,
                'name': 'A manually assigned name',
                'short_name': 'A manually assigned short name',
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
        self, institution_data: dict[str, str], expected_institution: dict[str, str]
    ):
        """
        Test that given a correct ROR ID, the `Institution` model
        retrieves data correctly.
        """
        institution = Institution(**institution_data, labs=[])

        for key, value in expected_institution.items():
            assert getattr(institution, key) == value

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

    def test_delivery_dir(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, full_db: dict
    ):
        """
        Test that the `Lab` model correctly sets the `delivery_dir`
        attribute.
        """
        # Change the delivery_parent_dir to tmp_path
        monkeypatch.setenv('DELIVERY_PARENT_DIR', str(tmp_path))

        # Get the two necessary objects for a Lab
        institution: Institution = full_db['institution']
        pi: Person = full_db['person']

        # Define and create the expected delivery directory and group.
        # Even though this is defined already by the full_db fixture,
        # define it manually for the sake of test completeness
        delivery_dir = tmp_path / 'ahmed_said'
        delivery_dir.mkdir(exist_ok=True)
        group = f'said_lab'

        # tmp_path.group() does not work, so monkeypatch it to return a
        # custom value
        monkeypatch.setattr('pathlib.Path.group', lambda s: group)

        # Define the expected lab and create the Lab object
        expected_lab = {
            'delivery_dir': str(delivery_dir),
            'group': group,
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

    # TODO: this is essentially a copy-paste of the TestPersonModel.
    # Figure out a better way to do this
    @pytest.mark.parametrize(
        argnames=['project_id', 'expected_project_id'],
        argvalues=[('\n\tscp99-000 ', 'SCP99-000')],
    )
    def test_valid_project_id(
        self, full_db: dict, project_id: str, expected_project_id: str
    ):
        """
        Test that the `Library` model cleans the library ID.
        """
        project = Project(id=project_id, lab=full_db['lab'])
        assert project.id == expected_project_id

    def test_invalid_project_id(self, full_db: dict):
        """
        Test that the `Library` model raises error with invalid library ID.
        """
        with pytest.raises(Abort):
            Project(id='fake-id', lab=full_db['lab'])


class TestPersonModel:
    """
    Tests for the `Person` model.
    """

    def test_name(self):
        """
        Test that the `Person` model correctly sets the `name` attribute
        with a poorly formatted name.
        """
        person = Person(first_name='\n\tahmed ', last_name='\n\tsaid ')
        assert person.name == 'Ahmed Said'

    def test_valid_orcid(self):
        """
        Test that the `Person` model acceps the ORCID.
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
        argnames=['orcid', 'expected_error'],
        argvalues=[
            ('fake-orcid', '.*pattern.*'),
            ('9999-9999-9999-9999', '.*database.*'),
        ],
    )
    def test_invalid_orcid(self, orcid: str, expected_error: str):
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

    pass


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
        argvalues=[('\n\tsc9900000 ', 'SC9900000')],
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
