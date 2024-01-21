from pathlib import Path
from typing import Any

import pandas as pd
from pytest import MonkeyPatch, fixture
from sqlalchemy.orm import Session, sessionmaker
from yaml import dump as dump_yml

from scbl_utils.core.db import db_session
from scbl_utils.db_models.bases import Base
from scbl_utils.db_models.data import (
    DataSet,
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
    Create a temporary database path for testing.
    """
    return tmp_path / 'test.db'


@fixture
def test_db_session(db_path: Path) -> sessionmaker[Session]:
    """
    Create a database session for testing.
    """
    Session = db_session(Base, drivername='sqlite', database=str(db_path))
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
def complete_db_objects(delivery_parent_dir: Path) -> dict[str, Base]:
    """
    Create valid, interlinked objects for each table in the database.
    Useful for testing models that depend on other models, so you have
    the necessary models made without having to make them in the test
    itself. Note that these models are not added to a database.
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
    institution = Institution(
        ror_id='021sy4w91',
        short_name='JAX-GM',
        email_format='{first_name}.{last_name}@jax.org',
    )
    person = Person(
        first_name='ahmed',
        last_name='said',
        email='ahmed.said@jax.org',
        orcid='0009-0008-3754-6150',
        institution=institution,
    )

    # Create delivery directory for the lab before creating the lab
    # itself
    (
        delivery_parent_dir / f'{person.first_name.lower()}_{person.last_name.lower()}'
    ).mkdir()

    lab = Lab(institution=institution, pi=person)
    project = Project(id='SCP99-000', lab=lab, people=[person])
    platform = Platform(name='platform')
    data_set = DataSet(
        name='data_set',
        project=project,
        platform=platform,
        ilab_request_id='ilab_request_id',
        submitter=person,
    )
    sample = Sample(name='sample', data_set=data_set, tag=tag)
    library = Library(
        id='SC9900000', data_set=data_set, library_type=library_type, status='status'
    )
    sequencing_run = SequencingRun(id='99-scbct-000', libraries=[library])

    return {
        'platform': platform,
        'library_type': library_type,
        'tag': tag,
        'institution': institution,
        'person': person,
        'lab': lab,
        'project': project,
        'data_set': data_set,
        'sample': sample,
        'library': library,
        'sequencing_run': sequencing_run,
    }


@fixture
def valid_data_dir(tmp_path: Path, delivery_parent_dir: Path) -> Path:
    """
    Create valid CSVs that can be passed to init-db for database
    initialization. Also returns a dict mapping the relationship of labs
    to institutions and PIs, since this is the key feature of init-db.
    """
    data_dir = tmp_path / 'data'
    data_dir.mkdir()

    # Create the data. Note missing values, which will be handled by
    # init-db
    dfs: dict[str, pd.DataFrame] = {}

    institutions = {
        'ror_id': ['02der9h97', '021sy4w91', None],
        'name': [
            None,
            'Jackson Laboratory for Mammalian Genetics',
            'Jackson Laboratory for Genomic Medicine',
        ],
        'short_name': [None, 'JAX-MG', 'JAX-GM'],
        'country': [None, None, None],
        'state': [None, None, 'CT'],
        'city': [None, None, 'Farmington'],
        'email_format': [
            '{first_name}.{last_name}@uconn.edu',
            '{first_name}.{last_name}@jax.org',
            '{first_name}.{last_name}@jax.org',
        ],
    }
    dfs['institution.csv'] = pd.DataFrame(institutions)

    labs = {
        'lab.pi.first_name': ['Ahmed', 'John', 'Jane'],
        'lab.pi.last_name': ['Said', 'Doe', 'Foe'],
        'lab.pi.email': ['ahmed.said@jax.org', 'john.doe@jax.org', 'jane.foe@jax.org'],
        'lab.pi.orcid': ['0009-0008-3754-6150', None, None],
        'lab.institution.name': [
            'Jackson Laboratory for Genomic Medicine',
            'Jackson Laboratory for Mammalian Genetics',
            'Jackson Laboratory for Genomic Medicine',
        ],
        'lab.name': [None, 'Service Lab', None],
        'lab.delivery_dir': [None, 'service_lab', None],
    }
    for directory in ('ahmed_said', 'service_lab', 'jane_foe'):
        (delivery_parent_dir / directory).mkdir()
    dfs['lab.csv'] = pd.DataFrame(labs)

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
    dfs['library_type.csv'] = pd.DataFrame({'name': library_types})

    people = {
        'person.first_name': ['Ahmed', 'John', 'Jane'],
        'person.last_name': ['Said', 'Doe', 'Foe'],
        'person.email': ['ahmed.said@jax.org', 'john.doe@jax.org', 'jane.foe@jax.org'],
        'person.orcid': ['0009-0008-3754-6150', None, None],
        'person.institution.name': [
            'Jackson Laboratory for Genomic Medicine',
            'Jackson Laboratory for Mammalian Genetics',
            'Jackson Laboratory for Genomic Medicine',
        ],
    }
    dfs['person.csv'] = pd.DataFrame(people)

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
    dfs['platform.csv'] = pd.DataFrame({'name': platforms})

    # TODO: get this from 10X themselves for more recent?
    dfs['tag.csv'] = pd.read_csv(
        'https://raw.githubusercontent.com/TheJacksonLaboratory/nf-tenx/main/assets/tags.csv'
    ).rename(
        columns={
            'tag_id': 'id',
            'tag_name': 'name',
            'tag_sequence': 'sequence',
            '5p_offset': 'five_prime_offset',
        }
    )

    for filename, df in dfs.items():
        df.to_csv(data_dir / filename, index=False)

    return data_dir


@fixture
def table_relationships(valid_data_dir: Path):
    """
    Return the relationships between labs, institutions, and PIs for
    testing. Can be easily extended for more relationships
    """
    # Read the tables and rename the 'person' table to 'pi' because a
    # Lab has a PI, not a Person
    tables = ('institution', 'lab', 'person')
    dfs = {table: pd.read_csv(valid_data_dir / f'{table}.csv') for table in tables}
    dfs['pi'] = dfs['person'].copy()

    # Add 1-indexed IDs to the tables that will match the IDs in the
    # database
    for table, df in dfs.items():
        df[f'{table}_id'] = dfs[table].index + 1

    # This maps a combination of tables to the columns used to join
    # those two tables. For example, the 'institution_name' column in
    # the lab table is the same as the 'name' column in the institution
    # table.
    table_relations = {
        ('person', 'institution'): (['person.institution.name', 'name']),
        ('lab', 'institution'): (['lab.institution.name'], ['name']),
        ('lab', 'pi'): (
            ['lab.pi.first_name', 'lab.pi.last_name', 'lab.pi.email', 'lab.pi.orcid'],
            ['person.first_name', 'person.last_name', 'person.email', 'person.orcid'],
        ),
    }

    # Merge the tables
    mappings = {}
    for (left_table, right_table), (left_cols, right_cols) in table_relations.items():
        mappings[(left_table, right_table)] = dfs[left_table].merge(
            dfs[right_table], how='left', left_on=left_cols, right_on=right_cols
        )

    return mappings


@fixture
def n_rows_per_table(valid_data_dir: Path):
    """ """
    dfs = {path.stem: pd.read_csv(path) for path in valid_data_dir.iterdir()}
    return {table: df.shape[0] for table, df in dfs.items()}
