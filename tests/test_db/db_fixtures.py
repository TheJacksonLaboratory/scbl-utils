from pathlib import Path

import pandas as pd
from pytest import MonkeyPatch, fixture
from sqlalchemy.orm import Session, sessionmaker
from yaml import dump as dump_yml

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
def db_path(tmp_path: Path) -> Path:
    """
    Create a temporary database for testing.
    """
    db_path = tmp_path / 'test.db'
    return db_path


@fixture
def db_session(db_path: Path) -> sessionmaker[Session]:
    """
    Create a database session for testing.
    """
    Session = new_db_session(Base, drivername='sqlite', database=str(db_path))
    return Session


@fixture
def delivery_parent_dir(monkeypatch: MonkeyPatch, tmp_path: Path) -> Path:
    """
    Create a temporary delivery parent directory for testing and set
    the environment variable DELIVERY_PARENT_DIR to it. Also change
    the return value of `pathlib.Path.group` to 'test_group' to avoid
    messing with groups on the system.
    """
    delivery_parent_dir = tmp_path / 'delivery'
    delivery_parent_dir.mkdir()

    monkeypatch.setenv('DELIVERY_PARENT_DIR', str(delivery_parent_dir))
    monkeypatch.setattr('pathlib.Path.group', lambda s: 'test_group')

    return delivery_parent_dir


@fixture
def config_dir(tmp_path: Path, db_path: Path) -> Path:
    """
    Create a temporary configuration directory for testing.
    """
    config_dir = tmp_path / '.config' / 'db'
    config_dir.mkdir(parents=True)

    db_config = {'drivername': 'sqlite', 'database': str(db_path)}

    config_path = config_dir / 'db-spec.yml'
    with config_path.open('w') as f:
        dump_yml(db_config, f)

    return config_dir.parent


@fixture
def full_db(delivery_parent_dir: Path) -> dict:
    """
    Create valid, interlinked objects for each table in the database.
    Useful for testing models that depend on other models, so you have
    the necessary models made without having to make them in the test
    itself.
    """
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

    # Create delivery directory for the lab before creating the lab
    # itself
    (
        delivery_parent_dir / f'{person.first_name.lower()}_{person.last_name.lower()}'
    ).mkdir()

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


@fixture
def valid_data(tmp_path: Path, delivery_parent_dir: Path) -> tuple[Path, dict]:
    """
    Create a valid CSVs that can be passed to init-db for database
    initialization. Also returns a dict mapping the relationship of labs
    to institutions and PIs, since this is the key feature of init-db.
    """
    data_dir = tmp_path / 'data'
    data_dir.mkdir()

    for directory in ('ahmed_said', 'service_lab'):
        (delivery_parent_dir / directory).mkdir()

    # Create the data. Note missing values, which will be handled by
    # init-db
    input_dfs: dict[str, pd.DataFrame] = {}

    institutions = {
        'ror_id': ['02der9h97', '021sy4w91', None],
        'name': [
            None,
            'The Jackson Laboratory for Mammalian Genetics',
            'The Jackson Laboratory for Genomic Medicine',
        ],
        'short_name': [None, 'JAX-MG', 'JAX-GM'],
        'country': [None, None, None],
        'state': [None, None, 'CT'],
        'city': [None, None, 'Farmington'],
    }
    input_dfs['institution.csv'] = pd.DataFrame(institutions)

    labs = {
        'pi_first_name': ['Ahmed', 'John'],
        'pi_last_name': ['Said', 'Doe'],
        'pi_email': ['ahmed.said@jax.org', 'john.doe@jax.org'],
        'pi_orcid': ['0009-0008-3754-6150', None],
        'institution_name': [
            'The Jackson Laboratory for Genomic Medicine',
            'The Jackson Laboratory for Mammalian Genetics',
        ],
        'name': [None, 'Service Lab'],
        'delivery_dir': [None, 'service_lab'],
    }
    input_dfs['lab.csv'] = pd.DataFrame(labs)

    library_types = [
        'Antibody Capture',
        'Chromatin Accessibility',
        'CRISPR Guide Capture',
        'CytAssist Gene Expression',
        'Gene Expression',
        'Immune Profiling',
        'Multiplexing Capture',
        'Spatial Gene Expression',
    ]
    input_dfs['librarytype.csv'] = pd.DataFrame({'name': library_types})

    people = {
        'first_name': ['ahmed', 'john', 'jane'],
        'last_name': ['said', 'doe', 'doe'],
        'email': ['ahmed.said@jax.org', 'john.doe@jax.org', 'jane.doe@jax.org'],
        'orcid': ['0009-0008-3754-6150', None, None],
    }
    input_dfs['person.csv'] = pd.DataFrame(people)

    # TODO: can we use the canonical 10x names for these platforms?
    platforms = [
        "3' RNA",
        "3' RNA-HT",
        "5' RNA",
        "5' RNA-HT",
        "5' VDJ",
        'ATAC',
        'ATAC v2',
        'Automated RNA',
        'CellPlex',
        'Cell Surface',
        'CUT and Tag',
        'Flex',
        'HTO',
        'LMO',
        'Multiome',
        'Visium FF',
        'Visium FFPE',
        'Visium CytAssist FFPE',
    ]
    input_dfs['platform.csv'] = pd.DataFrame({'name': platforms})

    # TODO: get this from 10X themselves for more recent?
    input_dfs['tag.csv'] = pd.read_csv(
        'https://raw.githubusercontent.com/TheJacksonLaboratory/nf-tenx/main/assets/tags.csv'
    ).rename(
        columns={
            'tag_id': 'id',
            'tag_name': 'name',
            'tag_sequence': 'sequence',
            '5p_offset': 'five_prime_offset',
        }
    )

    for filename, df in input_dfs.items():
        df.to_csv(data_dir / filename, index=False)

    return data_dir, {
        1: {'pi_id': 1, 'institution_id': 3},
        2: {'pi_id': 2, 'institution_id': 2},
    }
