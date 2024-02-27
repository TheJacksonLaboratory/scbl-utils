from pathlib import Path

from pandas import DataFrame
from pytest import MonkeyPatch, fixture

from scbl_utils.db.orm.models.data import *
from scbl_utils.db.orm.models.entities import *
from scbl_utils.db.orm.models.platforms.chromium import ChromiumDataSet


@fixture
def institution() -> Institution:
    """
    Create a valid Institution object for testing.
    """
    return Institution(
        ror_id='021sy4w91',
        short_name='JAX-MG',
        email_format='{first_name}.{last_name}@jax.org',
    )


@fixture
def person(institution: Institution) -> Person:
    """
    Create a valid Person object for testing.
    """
    return Person(
        first_name='ahmed',
        last_name='said',
        email='ahmed.said@jax.org',
        orcid='0009-0008-3754-6150',
        institution=institution,
    )


@fixture
def delivery_parent_dir(monkeypatch: MonkeyPatch, tmp_path: Path) -> Path:
    """
    Create a temporary delivery parent directory for testing and set
    the environment variable delivery_parent_dir to it. Also change
    the return value of `pathlib.Path.group` to 'test_group' to avoid
    messing with groups on the system.
    """
    delivery_parent_dir = tmp_path / 'delivery'
    delivery_parent_dir.mkdir()

    monkeypatch.setenv('delivery_parent_dir', str(delivery_parent_dir))
    monkeypatch.setattr('pathlib.Path.group', lambda s: 'test_group')

    return delivery_parent_dir


@fixture
def lab(delivery_parent_dir: Path, institution: Institution, person: Person) -> Lab:
    """Create a valid Lab object for testing"""
    (
        delivery_parent_dir / f'{person.first_name.lower()}_{person.last_name.lower()}'
    ).mkdir()
    return Lab(institution=institution, pi=person)


@fixture
def institution_as_df(institution: Institution) -> DataFrame:
    """Create a DataFrame with the Institution data"""

    return DataFrame(
        [
            {
                'ror_id': institution.ror_id,
                'name': institution.name,
                'short_name': institution.short_name,
                'email_format': institution.email_format,
                'city': institution.city,
                'state': institution.state,
                'country': institution.country,
            }
        ]
    )


@fixture
def person_as_df(person: Person) -> DataFrame:
    """Create a DataFrame with the person data"""
    return DataFrame(
        [
            {
                'first_name': person.first_name,
                'last_name': person.last_name,
                'email': person.email,
                'orcid': person.orcid,
                'institution': person.institution.ror_id,
            }
        ]
    )
