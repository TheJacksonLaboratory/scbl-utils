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
    the necessary parent objects made without having to make them in the
    test itself. Note that these models are not added to a database.
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
def db_data(delivery_parent_dir: Path) -> dict[str, pd.DataFrame]:
    """
    Create dummy data for insertion into the database.
    """
    dfs: dict[str, pd.DataFrame] = {}

    institutions = {
        'institution.ror_id': ['02der9h97', '021sy4w91', None],
        'institution.name': [
            None,
            'Jackson Laboratory for Mammalian Genetics',
            'Jackson Laboratory for Genomic Medicine',
        ],
        'institution.short_name': [None, 'JAX-MG', 'JAX-GM'],
        'institution.country': [None, None, None],
        'institution.state': [None, None, 'CT'],
        'institution.city': [None, None, 'Farmington'],
        'institution.email_format': [
            r'{first_name}.{last_name}@uconn.edu',
            r'{first_name}.{last_name}@jax.org',
            r'{first_name}.{last_name}@jax.org',
        ],
    }
    dfs['institution'] = pd.DataFrame(institutions)

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
    dfs['lab'] = pd.DataFrame(labs)

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
    dfs['library_type'] = pd.DataFrame({'library_type.name': library_types})

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
    dfs['person'] = pd.DataFrame(people)

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
    dfs['platform'] = pd.DataFrame({'platform.name': platforms})

    # TODO: get this from 10X themselves for more recent information?
    dfs['tag'] = pd.read_csv(
        'https://raw.githubusercontent.com/TheJacksonLaboratory/nf-tenx/main/assets/tags.csv'
    ).rename(
        columns={
            'tag_id': 'tag.id',
            'tag_name': 'tag.name',
            'tag_sequence': 'tag.sequence',
            '5p_offset': 'tag.five_prime_offset',
        }
    )

    projects = {
        'project.id': ['SCP99-000', 'SCP99-001', 'SCP99-002'],
        'project.lab.pi.name': ['Ahmed Said', 'John Doe', 'Jane Foe'],
        'project.lab.pi.email': [
            'ahmed.said@jax.org',
            'john.doe@jax.org',
            'jane.foe@jax.org',
        ],
        'project.lab.pi.orcid': ['0009-0008-3754-6150', None, None],
    }
    dfs['project'] = pd.DataFrame(projects)

    data_sets = {
        'data_set.name': ['data_set_0', 'data_set_1', 'data_set_2', 'data_set_3'],
        'data_set.project.id': ['SCP99-000', 'SCP99-000', 'SCP99-001', 'SCP99-002'],
        'data_set.platform.name': ['3\' RNA', 'Multiome', 'Flex', 'CellPlex'],
        'data_set.ilab_request_id': [
            'ilab_request_id_0',
            'ilab_request_id_0',
            'ilab_request_id_1',
            'ilab_request_id_2',
        ],
        'data_set.submitter.name': [None, 'Ahmed Said', 'Dohn Joe', 'Jane Foe'],
        'data_set.submitter.email': [
            'ahmed.said@jax.org',
            None,
            'dohn.joe@jax.org',
            'jane.foe@jax.org',
        ],
        'data_set.date_submitted': [
            '1999-01-01',
            '1999-02-01',
            '1999-02-01',
            '1999-02-01',
        ],
    }
    dfs['data_set'] = pd.DataFrame(data_sets)

    samples = {
        'sample.name': [
            'sample_0',
            'sample_1',
            'sample_2',
            'sample_3',
            'sample_4',
            'sample_5',
            'sample_6',
        ],
        'sample.data_set.name': [
            'data_set_0',
            'data_set_1',
            'data_set_2',
            'data_set_2',
            'data_set_3',
            'data_set_3',
            'data_set_3',
        ],
        'sample.data_set.ilab_request_id': [
            'ilab_request_id_0',
            'ilab_request_id_0',
            'ilab_request_id_1',
            'ilab_request_id_1',
            'ilab_request_id_2',
            'ilab_request_id_2',
            'ilab_request_id_2',
        ],
        'sample.data_set.date_submitted': [
            '1999-01-01',
            '1999-01-01',
            '1999-02-01',
            '1999-02-01',
            '1999-02-01',
            '1999-02-01',
            '1999-02-01',
        ],
        'sample.data_set.project.id': [
            'SCP99-000',
            'SCP99-000',
            'SCP99-001',
            'SCP99-001',
            'SCP99-002',
            'SCP99-002',
            'SCP99-002',
        ],
        'sample.data_set.submitter.name': [
            None,
            'Ahmed Said',
            'Dohn Joe',
            'Dohn Joe',
            'Jane Foe',
            'Jane Foe',
            'Jane Foe',
        ],
    }  # TODO: you are here, verify github copilot's suggestions here
    dfs['sample'] = pd.DataFrame(samples)

    # TODO: the mapping between sequencing run and library is probably
    # not realistic due to biotechnological considerations. The
    # perfectionist in me wants to make this more realistic
    sequencing_runs = ['99-scbct-000', '99-scbct-001']
    dfs['sequencing_run'] = pd.DataFrame({'sequencing_run.id': sequencing_runs})

    libraries = {
        'library.id': [
            'SC9900000',
            'SC9900001',
            'SC9900002',
            'SC9900003',
            'SC9900004',
            'SC9900005',
        ],
        'library.data_set.name': [
            'data_set_0',
            'data_set_1',
            'data_set_1',
            'data_set_2',
            'data_set_3',
            'data_set_3',
        ],
        'library.data_set.ilab_request_id': [
            'ilab_request_id_0',
            'ilab_request_id_0',
            'ilab_request_id_0',
            'ilab_request_id_1',
            'ilab_request_id_2',
            'ilab_request_id_2',
        ],
        'library.data_set.date_submitted': [
            '1999-01-01',
            '1999-01-01',
            '1999-01-01',
            '1999-02-01',
            '1999-02-01',
            '1999-02-01',
        ],
        'library.data_set.project.id': [
            'SCP99-000',
            'SCP99-000',
            'SCP99-000',
            'SCP99-001',
            'SCP99-002',
            'SCP99-002',
        ],
        'library.data_set.submitter.name': [
            None,
            'Ahmed Said',
            'Ahmed Said',
            'Dohn Joe',
            'Jane Foe',
            'Jane Foe',
        ],
        'library.data_set.submitter.email': [
            'ahmed.said@jax.org',
            None,
            None,
            'dohn.joe@jax.org',
            'jane.foe@jax.org',
            'jane.foe@jax.org',
        ],
        'library.data_set.platform.name': [
            '3\' RNA',
            'Multiome',
            'Multiome',
            'Flex',
            'CellPlex',
            'CellPlex',
        ],
        'library.library_type.name': [
            'Gene Expression',
            'Gene Expression',
            'Chromatin Accessibility',
            'Gene Expression',
            'Gene Expression',
            'Multiplexing Capture',
        ],
        'library.status': [
            'completed',
            'sequencing',
            'sequencing',
            'library complete',
            'cDNA',
            'cDNA',
        ],
        'library.sequencing_run.id': [
            '99-scbct-000',
            '99-scbct-001',
            '99-scbct-001',
            None,
            None,
            None,
        ],
    }
    dfs['library'] = pd.DataFrame(libraries)

    return dfs


def data_dir(db_data: dict[str, pd.DataFrame], tmp_path: Path):
    """
    Create a data directory with all the data necessary to initialize
    and fill a database.
    """
    data_dir = tmp_path / 'data'
    data_dir.mkdir()

    for tablename, df in db_data.items():
        df.to_csv(data_dir / f'{tablename}.csv', index=False)

    return data_dir


@fixture
def table_relationships(db_data: dict[str, pd.DataFrame]):
    """
    Return the relationships between labs, institutions, and PIs for
    testing. Can be easily extended for more relationships
    """
    dfs = db_data.copy()

    # Add 1-indexed IDs to the tables that will match the IDs in the
    # database
    for table, df in dfs.items():
        df[f'{table}_id'] = dfs[table].index + 1

    # This maps a combination of tables to the columns used to join
    # those two tables. For example, the 'lab.institution_name' column
    # in the `lab`` table is the same as the 'institution.name' column
    # in the `institution` table.
    table_relations = {
        ('person', 'institution'): (['person.institution.name', 'institution.name']),
        ('lab', 'institution'): (['lab.institution.name'], ['institution.name']),
        ('lab', 'person'): (
            ['lab.pi.first_name', 'lab.pi.last_name', 'lab.pi.email', 'lab.pi.orcid'],
            ['person.first_name', 'person.last_name', 'person.email', 'person.orcid'],
        ),
        ('project', 'lab'): (
            ['project.lab.pi.name', 'project.lab.pi.email', 'project.lab.pi.orcid'],
            ['lab.pi.name', 'lab.pi.email', 'lab.pi.orcid'],
        ),
        ('data_set', 'project'): (['data_set.project.id'], ['project.id']),
        ('data_set', 'person'): (
            ['data_set.submitter.name', 'data_set.submitter.email'],
            ['person.name', 'person.email'],
        ),
        ('data_set', 'platform'): (['data_set.platform.name'], ['platform.name']),
        ('sample', 'data_set'): (
            [
                'sample.data_set.name',
                'sample.data_set.ilab_request_id',
                'sample.data_set.date_submitted',
            ],
            ['data_set.name', 'data_set.ilab_request_id', 'data_set.date_submitted'],
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
def n_rows_per_table(db_data: Path):
    """ """
    dfs = {path.stem: pd.read_csv(path) for path in db_data.iterdir()}
    return {table: df.shape[0] for table, df in dfs.items()}
