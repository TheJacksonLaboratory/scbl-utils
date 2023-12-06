from pathlib import Path

from pytest import MonkeyPatch, fixture

from scbl_utils.core import new_db_session
from scbl_utils.db_models.bases import Base
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
from scbl_utils.db_models.definitions import LibraryType, Platform, Tag


@fixture
def tmp_db_session(tmp_path: Path):
    """
    Create a temporary database for testing.
    """
    db_path = tmp_path / 'test.db'
    Session = new_db_session(
        Base, drivername='sqlite', database=str(db_path.absolute())
    )
    return Session


@fixture
def full_db(monkeypatch: MonkeyPatch, tmp_path: Path) -> dict:
    """
    Create valid, interlinked objects for each table in the database.
    """
    monkeypatch.setenv('DELIVERY_PARENT_DIR', str(tmp_path))
    (tmp_path / 'ahmed_said').mkdir()
    monkeypatch.setattr('pathlib.Path.group', lambda s: 'said_lab')

    # Definition models
    platform = Platform(name='platform')
    library_type = LibraryType(name='library_type')
    tag = Tag(
        id='BC000',
        name='tag',
        tag_type='tag_type',
        sequence='ACTG',
        pattern='5P(BC)',
        five_prime_offset=1,
        read='R2',
    )

    # Data models
    institution = Institution(ror_id='021sy4w91', short_name='JAX-GM')
    person = Person(
        first_name='ahmed',
        last_name='said',
        email='ahmed.said@jax.org',
        orcid='0009-0008-3754-6150',
    )
    lab = Lab(institution=institution, pi=person)
    project = Project(id='SCP99-000', lab=lab, people=[person])
    platform = Platform(name='platform')
    experiment = Experiment(name='experiment', project=project, platform=platform)
    sample = Sample(name='sample', experiment=experiment, tag=tag)
    library = Library(id='SC9900000', experiment=experiment, library_type=library_type)
    sequencing_run = SequencingRun(id='99-scbct-000', libraries=[library])

    return {
        'platform': platform,
        'library_type': library_type,
        'tag': tag,
        'institution': institution,
        'person': person,
        'lab': lab,
        'project': project,
        'experiment': experiment,
        'sample': sample,
        'library': library,
        'sequencing_run': sequencing_run,
    }
