from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pytest import MonkeyPatch, fixture
from sqlalchemy import inspect
from sqlalchemy.orm import Session, sessionmaker

from scbl_utils.db.core import db_session
from scbl_utils.db.orm.base import Base
from scbl_utils.db.orm.models.data import *
from scbl_utils.db.orm.models.platforms.chromium import *
from scbl_utils.db.orm.models.platforms.xenium import *


@fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """
    Create a temporary database path for testing.
    """
    return tmp_path / 'test.db'


@fixture
def tmp_db_session(tmp_db_path: Path) -> sessionmaker[Session]:
    """
    Create a database session for testing.
    """
    Session = db_session(Base, drivername='sqlite', database=str(tmp_db_path))
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

    monkeypatch.setattr('pathlib.Path.group', lambda s: 'test_group')

    return delivery_parent_dir


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


# TODO: all of the above assume that a '.' is the OBJECT_SEP_CHAR. This
# should be changed so updating the code is easier
@fixture
def db_data(delivery_parent_dir: Path) -> dict[str, pd.DataFrame]:
    """
    Create dummy data for insertion into the database.
    """
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
            r'{first_name}.{last_name}@uconn.edu',
            r'{first_name}.{last_name}@jax.org',
            r'{first_name}.{last_name}@jax.org',
        ],
    }
    dfs['institution'] = pd.DataFrame(institutions)

    labs = {
        'pi.first_name': ['Ahmed', 'John', 'Jane'],
        'pi.last_name': ['Said', 'Doe', 'Foe'],
        'pi.email': ['ahmed.said@jax.org', 'john.doe@jax.org', 'jane.foe@jax.org'],
        'pi.orcid': ['0009-0008-3754-6150', None, None],
        'institution.name': [
            'Jackson Laboratory for Genomic Medicine',
            'Jackson Laboratory for Mammalian Genetics',
            'Jackson Laboratory for Genomic Medicine',
        ],
        'name': [None, 'Service Lab', None],
        'delivery_dir': [None, 'service_lab', None],
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
    dfs['library_type'] = pd.DataFrame({'name': library_types})

    people = {
        'first_name': ['Ahmed', 'John', 'Jane', 'Dohn'],
        'last_name': ['Said', 'Doe', 'Foe', 'Joe'],
        'email': [
            'ahmed.said@jax.org',
            'john.doe@jax.org',
            'jane.foe@jax.org',
            'dohn.joe@jax.org',
        ],
        'orcid': ['0009-0008-3754-6150', None, None, None],
        'institution.name': [
            'Jackson Laboratory for Genomic Medicine',
            'Jackson Laboratory for Mammalian Genetics',
            'Jackson Laboratory for Genomic Medicine',
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
    dfs['platform'] = pd.DataFrame({'name': platforms})

    # TODO: get this from 10X themselves for more recent information?
    dfs['tag'] = (
        pd.read_csv(
            'https://raw.githubusercontent.com/TheJacksonLaboratory/nf-tenx/main/assets/tags.csv'
        )
        .rename(
            columns={
                'tag_id': 'id',
                'tag_name': 'name',
                'tag_sequence': 'sequence',
                '5p_offset': 'five_prime_offset',
            }
        )
        .replace(np.nan, None)
    )

    projects = {
        'id': ['SCP99-000', 'SCP99-001', 'SCP99-002'],
        'lab.pi.first_name': ['Ahmed', 'John', 'Jane'],
        'lab.pi.last_name': ['Said', 'Doe', 'Foe'],
        'lab.pi.email': [
            'ahmed.said@jax.org',
            'john.doe@jax.org',
            'jane.foe@jax.org',
        ],
        'lab.pi.orcid': ['0009-0008-3754-6150', None, None],
    }
    dfs['project'] = pd.DataFrame(projects)

    # TODO: eventually, try removing certain cells to see how it's handled
    data_sets = {
        'name': ['data_set_0', 'data_set_1', 'data_set_2', 'data_set_3'],
        'project.id': ['SCP99-000', 'SCP99-000', 'SCP99-001', 'SCP99-002'],
        'platform.name': ['3\' RNA', 'Multiome', 'Flex', 'CellPlex'],
        'ilab_request_id': [
            'ilab_request_id_0',
            'ilab_request_id_0',
            'ilab_request_id_1',
            'ilab_request_id_2',
        ],
        'submitter.first_name': ['Ahmed', 'Ahmed', 'Dohn', 'Jane'],
        'submitter.last_name': ['Said', 'Said', 'Joe', 'Foe'],
        'submitter.email': [
            'ahmed.said@jax.org',
            'ahmed.said@jax.org',
            'dohn.joe@jax.org',
            'jane.foe@jax.org',
        ],
        'submitter.orcid': ['0009-0008-3754-6150', '0009-0008-3754-6150', None, None],
        'date_submitted': [
            date.fromisoformat('1999-01-01'),
            date.fromisoformat('1999-02-01'),
            date.fromisoformat('1999-02-01'),
            date.fromisoformat('1999-02-01'),
        ],
    }
    dfs['data_set'] = pd.DataFrame(data_sets)

    samples = {
        'name': [
            'sample_0',
            'sample_1',
            'sample_2',
            'sample_3',
            'sample_4',
            'sample_5',
            'sample_6',
        ],
        'data_set.name': [
            'data_set_0',
            'data_set_1',
            'data_set_2',
            'data_set_2',
            'data_set_3',
            'data_set_3',
            'data_set_3',
        ],
        'data_set.ilab_request_id': [
            'ilab_request_id_0',
            'ilab_request_id_0',
            'ilab_request_id_1',
            'ilab_request_id_1',
            'ilab_request_id_2',
            'ilab_request_id_2',
            'ilab_request_id_2',
        ],
        'data_set.date_submitted': [
            date.fromisoformat('1999-01-01'),
            date.fromisoformat('1999-02-01'),
            date.fromisoformat('1999-02-01'),
            date.fromisoformat('1999-02-01'),
            date.fromisoformat('1999-02-01'),
            date.fromisoformat('1999-02-01'),
            date.fromisoformat('1999-02-01'),
        ],
        'data_set.project.id': [
            'SCP99-000',
            'SCP99-000',
            'SCP99-001',
            'SCP99-001',
            'SCP99-002',
            'SCP99-002',
            'SCP99-002',
        ],
        'data_set.submitter.first_name': [
            'Ahmed',
            'Ahmed',
            'Dohn',
            'Dohn',
            'Jane',
            'Jane',
            'Jane',
        ],
        'data_set.submitter.last_name': [
            'Said',
            'Said',
            'Joe',
            'Joe',
            'Foe',
            'Foe',
            'Foe',
        ],
        'data_set.submitter.email': [
            'ahmed.said@jax.org',
            'ahmed.said@jax.org',
            'dohn.joe@jax.org',
            'dohn.joe@jax.org',
            'jane.foe@jax.org',
            'jane.foe@jax.org',
            'jane.foe@jax.org',
        ],
        'data_set.platform.name': [
            '3\' RNA',
            'Multiome',
            'Flex',
            'Flex',
            'CellPlex',
            'CellPlex',
            'CellPlex',
        ],
    }
    dfs['sample'] = pd.DataFrame(samples)

    # TODO: the mapping between sequencing run and library is probably
    # not realistic due to biotechnological considerations. The
    # perfectionist in me wants to make this more realistic
    sequencing_runs = ['99-scbct-000', '99-scbct-001']
    dfs['sequencing_run'] = pd.DataFrame({'id': sequencing_runs})

    libraries = {
        'id': [
            'SC9900000',
            'SC9900001',
            'SC9900002',
            'SC9900003',
            'SC9900004',
            'SC9900005',
        ],
        'data_set.name': [
            'data_set_0',
            'data_set_1',
            'data_set_1',
            'data_set_2',
            'data_set_3',
            'data_set_3',
        ],
        'data_set.ilab_request_id': [
            'ilab_request_id_0',
            'ilab_request_id_0',
            'ilab_request_id_0',
            'ilab_request_id_1',
            'ilab_request_id_2',
            'ilab_request_id_2',
        ],
        'data_set.date_submitted': [
            date.fromisoformat('1999-01-01'),
            date.fromisoformat('1999-02-01'),
            date.fromisoformat('1999-02-01'),
            date.fromisoformat('1999-02-01'),
            date.fromisoformat('1999-02-01'),
            date.fromisoformat('1999-02-01'),
        ],
        'data_set.project.id': [
            'SCP99-000',
            'SCP99-000',
            'SCP99-000',
            'SCP99-001',
            'SCP99-002',
            'SCP99-002',
        ],
        'data_set.submitter.first_name': [
            'Ahmed',
            'Ahmed',
            'Ahmed',
            'Dohn',
            'Jane',
            'Jane',
        ],
        'data_set.submitter.last_name': ['Said', 'Said', 'Said', 'Joe', 'Foe', 'Foe'],
        'data_set.submitter.email': [
            'ahmed.said@jax.org',
            'ahmed.said@jax.org',
            'ahmed.said@jax.org',
            'dohn.joe@jax.org',
            'jane.foe@jax.org',
            'jane.foe@jax.org',
        ],
        'data_set.platform.name': [
            '3\' RNA',
            'Multiome',
            'Multiome',
            'Flex',
            'CellPlex',
            'CellPlex',
        ],
        'library_type.name': [
            'Gene Expression',
            'Gene Expression',
            'Chromatin Accessibility',
            'Gene Expression',
            'Gene Expression',
            'Multiplexing Capture',
        ],
        'status': [
            'completed',
            'sequencing',
            'sequencing',
            'library complete',
            'cDNA',
            'cDNA',
        ],
        'sequencing_run.id': [
            '99-scbct-000',
            '99-scbct-001',
            '99-scbct-001',
            None,
            None,
            None,
        ],
    }
    dfs['library'] = pd.DataFrame(libraries)

    for tablename, df in dfs.items():
        if 'id' not in df.columns:
            df['id'] = df.index + 1

        dfs[tablename] = df.rename(
            columns={col: f'{tablename}.{col}' for col in df.columns}
        )

    return dfs


@fixture
def data_dir(db_data: dict[str, pd.DataFrame], tmp_path: Path):
    """
    Create a data directory with all the data necessary to initialize
    and fill a database.
    """
    data_dir = tmp_path / 'data'
    data_dir.mkdir()

    for tablename, df in db_data.items():
        if not tablename in DATA_SCHEMAS:
            continue

        id_column = f'{tablename}.id'
        if id_column in DATA_SCHEMAS[tablename]['items']['properties']:
            df.to_csv(data_dir / f'{tablename}.csv', index=False)

        else:
            columns_to_write = [col for col in df.columns if col != f'{tablename}.id']
            df.to_csv(
                data_dir / f'{tablename}.csv', index=False, columns=columns_to_write
            )

    return data_dir


@fixture
def other_parent_names() -> dict[str, str]:
    """ """
    return {'data_set.submitter': 'person', 'lab.pi': 'person'}


@fixture
def table_relationships(
    db_data: dict[str, pd.DataFrame], other_parent_names: dict[str, str]
):
    """
    Return the relationships between children and parents in the data.
    """
    # TODO: there is clear repetition below
    table_relations = {}
    for parent_tablename in db_data:
        child_dfs = {
            other_tablename: other_df
            for other_tablename, other_df in db_data.items()
            if other_tablename != parent_tablename
            and other_df.columns.str.contains(
                f'{other_tablename}.{parent_tablename}.'
            ).any()
        }

        for child_tablename, child_df in child_dfs.items():
            child_reference_columns = [
                col for col in child_df.columns if parent_tablename in col
            ]
            parent_columns = [
                col.removeprefix(f'{child_tablename}.')
                for col in child_reference_columns
            ]

            table_relations[(child_tablename, parent_tablename)] = (
                child_reference_columns,
                parent_columns,
            )

    # Not everything will be caught by the above for-loop because
    # children may reference parents by a different name than the name
    # of the parent table
    for parent_name, actual_parent_type in other_parent_names.items():
        child_tablename, parent = parent_name.split('.')

        child_reference_columns = [
            col for col in db_data[child_tablename].columns if parent_name in col
        ]
        parent_columns = [
            col.replace(parent, actual_parent_type).removeprefix(f'{child_tablename}.')
            for col in child_reference_columns
        ]

        table_relations[(child_tablename, parent)] = (
            child_reference_columns,
            parent_columns,
        )

    # Merge the tables
    mappings = {}
    for (child_table, parent_table), (
        child_columns,
        parent_columns,
    ) in table_relations.items():
        mappings[(child_table, parent_table)] = db_data[child_table].merge(
            db_data[
                other_parent_names.get(f'{child_table}.{parent_table}', parent_table)
            ],
            how='left',
            left_on=child_columns,
            right_on=parent_columns,
        )

    return mappings
