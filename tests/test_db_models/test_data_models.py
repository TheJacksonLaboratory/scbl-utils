from datetime import date, timedelta
from pathlib import Path
from re import sub
from string import punctuation, whitespace

from email_validator.exceptions_types import EmailUndeliverableError
from pytest import mark, raises
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from typer import Abort

from scbl_utils.db_models.base import Base
from scbl_utils.db_models.data_metadata import DataSet, Project
from scbl_utils.db_models.disassociative import *
from scbl_utils.db_models.researcher_metadata import Institution, Lab, Person

from ..fixtures.db_fixtures import (
    complete_db_objects,
    db_path,
    delivery_parent_dir,
    test_db_session,
)


class TestInstitutionModel:
    """
    Tests for the `Institution` model.
    """

    ror_id = '02der9h97'
    manual_name = 'Manual Name'
    email_format = r'{first_name}.{last_name}@uconn.edu'
    correct_dataset = [
        (
            {'ror_id': whitespace + ror_id + whitespace, 'email_format': email_format},
            {
                'ror_id': ror_id,
                'name': 'University of Connecticut',
                'short_name': 'UConn',
                'country': 'US',
                'state': 'CT',
                'city': 'Storrs',
                'email_format': email_format,
            },
        ),
        (
            {
                'ror_id': whitespace + ror_id + whitespace,
                'name': whitespace + manual_name + whitespace,
                'short_name': whitespace + manual_name + whitespace,
                'country': 'country',
                'state': 'state',
                'city': 'city',
                'email_format': email_format,
            },
            {
                'ror_id': ror_id,
                'name': manual_name,
                'short_name': manual_name,
                'country': 'US',
                'state': 'CT',
                'city': 'Storrs',
                'email_format': email_format,
            },
        ),
    ]

    @mark.parametrize(
        argnames=['institution_data', 'expected_institution'], argvalues=correct_dataset
    )
    def test_ror_id(
        self,
        institution_data: dict[str, str],
        expected_institution: dict[str, str],
        test_db_session: sessionmaker[Session],
    ):
        """
        Test that given a correct ROR ID, the `Institution` model
        retrieves data correctly. Also tests string stripping. Note
        that this is the only time that `StrippedString` is tested,
        as all other models use the same `StrippedString` type.
        """
        institution = Institution(**institution_data, labs=[])

        with test_db_session.begin() as session:
            session.add(institution)

        with test_db_session.begin() as session:
            stmt = select(Institution)
            processed_institution = session.execute(stmt).scalar()

            for key, value in expected_institution.items():
                assert getattr(processed_institution, key) == value

    def test_invalid_ror_id(self):
        """
        Test that given an incorrect ROR ID, the `Institution` model
        throws an error.
        """
        with raises(Abort):
            Institution(
                ror_id='nonexistent_ror_id',
                email_format=r'{first_name}.{last_name}@jax.org',
            )

    def test_email_format_invalid_attribute(self):
        """
        Test that given an incorrect email format, the `Institution`
        model throws an error.
        """
        with raises(Abort):
            Institution(
                ror_id=self.ror_id, email_format=r'{non_existent_attribute}@jax.org'
            )

    def test_email_format_no_variables(self):
        with raises(Abort):
            Institution(ror_id=self.ror_id, email_format=r'contant_email@jax.org')

    def test_email_format_invalid_domain(self):
        with raises(EmailUndeliverableError):
            Institution(
                ror_id=self.ror_id, email_format=r'{first_name}.{last_name}@jax.abc'
            )


class TestLabModel:
    """
    Tests for the `Lab` model.
    """

    def test_auto_setting(self, complete_db_objects: dict, delivery_parent_dir: Path):
        """
        Test that the `Lab` model correctly sets the `delivery_dir` and
        `name` attributes when given the minimum required information.
        """
        # Get the two necessary objects for a Lab
        institution: Institution = complete_db_objects['institution']
        pi: Person = complete_db_objects['person']

        # Define the expected lab and create the Lab object. Note that
        # the group is hardcoded to 'test_group' because of the fixture
        # delivery_parent_dir, which is called by full_db
        expected_lab = {
            'delivery_dir': str(
                delivery_parent_dir / f'{pi.first_name.lower()}_{pi.last_name.lower()}'
            ),
            'group': 'test_group',
            'name': 'Ahmed Said Lab',
            'projects': [],
        }
        lab = Lab(institution=institution, pi=pi)

        for key, value in expected_lab.items():
            assert getattr(lab, key) == value


class TestProjectModel:
    """
    Tests for the `Project` model.
    """

    def test_invalid_project_id(self, complete_db_objects: dict):
        """
        Test that the `Project` model raises error with invalid project ID.
        """
        with raises(Abort):
            Project(id='invalid-id', lab=complete_db_objects['lab'])


class TestPersonModel:
    """
    Tests for the `Person` model.
    """

    def test_orcid(self, complete_db_objects: dict[str, Base]):
        """
        Test that the `Person` model accepts the ORCID, regardless of
        the number of dashes.
        """
        orcid = '0009-0008-3754-6150'
        n_dashes = orcid.count('-')
        people = (
            Person(
                first_name='Ahmed', last_name='Said', orcid=orcid.replace('-', '', i), institution=complete_db_objects['institution']  # type: ignore
            )
            for i in range(1, n_dashes + 1)
        )

        for person in people:
            assert person.orcid == orcid

    @mark.parametrize(
        argnames=[
            'orcid',
        ],
        argvalues=[
            ('invalid-orcid',),
            ('9999-9999-9999-9999',),
        ],
    )
    def test_invalid_orcid(self, orcid: str, complete_db_objects: dict[str, Base]):
        """
        Test that the `Person` model raises error with invalid ORCID.
        """
        # Get the necessary object for a Person
        institution: Institution = complete_db_objects['institution']  # type: ignore

        with raises(Abort):
            Person(
                first_name='Ahmed',
                last_name='Said',
                orcid=orcid,
                institution=institution,
            )

    def test_autoset_email(self, complete_db_objects: dict):
        """
        Test that the `Person` model correctly sets the email attribute
        when given the minimum required information.
        """
        # Get the necessary object for a Person
        institution: Institution = complete_db_objects['institution']

        # Initialize a Person with a last name with a space
        person = Person(
            first_name='Ahmed', last_name='Said Alaani', institution=institution
        )
        domain = institution.email_format.split("@")[1]

        assert person.email == f'ahmed.saidalaani@{domain}'
        assert person.email_auto_generated


class TestDataSetModel:
    """
    Tests for the `data_set` model.
    """

    def test_batch_id(self, complete_db_objects: dict):
        """
        Test that two `DataSet`s with the same date submitted and the
        same sample submitter have the same batch ID.
        """
        # Get the necessary objects for a DataSet
        project = complete_db_objects['project']
        platform = complete_db_objects['platform']
        submitter = complete_db_objects['person']

        # Also create the other necessary pieces of data
        n_data_sets = 4
        same_batch_data_set_names = (f'data_set_{i}' for i in range(n_data_sets - 2))
        diff_batch_data_set_names = [
            f'data_set_{i}' for i in range(n_data_sets - 2, n_data_sets)
        ]
        ilab_request_id = 'ilab_request_id'

        # Create two DataSets with the same date submitted and the same
        # sample submitter. Also test that the date_submitted is
        # automatically set to today's date.
        same_batch_ids = [
            DataSet(
                name=name,
                date_submitted=date.today(),
                project=project,
                platform=platform,
                ilab_request_id=ilab_request_id,
                submitter=submitter,
            ).batch_id
            for name in same_batch_data_set_names
        ]

        assert all(same_batch_ids[0] == batch_id for batch_id in same_batch_ids)

        # Also create a DataSet with a different date submitted
        data_set_2 = DataSet(
            name=diff_batch_data_set_names[0],
            date_submitted=date.today() - timedelta(days=1),
            project=project,
            platform=platform,
            ilab_request_id=ilab_request_id,
            submitter=submitter,
        )

        assert same_batch_ids[0] != data_set_2.batch_id

        # Also create a DataSet with a different sample submitter
        new_person = Person(
            first_name='new',
            last_name='person',
            institution=complete_db_objects['institution'],
        )
        data_set_3 = DataSet(
            name=diff_batch_data_set_names[1],
            project=project,
            platform=platform,
            ilab_request_id='ilab_request_id',
            submitter=new_person,
        )

        assert same_batch_ids[0] != data_set_3.batch_id
        assert data_set_2.batch_id != data_set_3.batch_id


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

    def test_library_id(self, complete_db_objects: dict):
        """
        Test that the `Library` model cleans the library ID.
        """
        data_set = complete_db_objects['data_set']
        library_type = complete_db_objects['library_type']
        library = Library(
            id=f'{whitespace}sc9900000{whitespace}',
            data_set=data_set,
            library_type=library_type,
            status='status',
        )
        assert library.id == 'SC9900000'

    def test_invalid_library_id(self, complete_db_objects: dict):
        """
        Test that the `Library` model raises error with invalid library ID.
        """
        data_set = complete_db_objects['data_set']
        library_type = complete_db_objects['library_type']
        with raises(Abort):
            data_set = complete_db_objects['data_set']
            Library(
                id='id', data_set=data_set, library_type=library_type, status='status'
            )
