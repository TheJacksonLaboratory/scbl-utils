# TODO: Remove the test_data directory, and create the output
# expected from init-db when using the valid_env fixture.
from pathlib import Path

import pandas as pd
from pytest import MonkeyPatch, fixture
from yaml import safe_dump as yml_safe_dump

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
    Useful for testing models that depend on other models, so you have
    the necessary models made without having to make them in the test
    itself.
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


@fixture
def valid_env(monkeypatch: MonkeyPatch, tmp_path: Path):
    """
    Create a valid environment that contains all the necessary items for
    database initialization. This includes valid data, a config dir, and
    monkey patches for the delivery directory and delivery directory
    groups.
    """
    # Create necessary directories
    necessary_dirs = (
        'data',
        'delivery/ahmed_said',
        'delivery/service_lab',
        '.config/db',
    )
    for directory in necessary_dirs:
        (tmp_path / directory).mkdir(exist_ok=True, parents=True)

    # Monkey patch delivery parent directory environment variable and
    # the return value for the function pathlib.Path.group
    delivery_parent_dir = tmp_path / 'delivery'
    monkeypatch.setenv('DELIVERY_PARENT_DIR', str(delivery_parent_dir))
    monkeypatch.setattr('pathlib.Path.group', lambda s: 'group')

    # Create the config file
    config = {'drivername': 'sqlite', 'database': str(tmp_path / 'test.db')}
    yml_config = yml_safe_dump(config)

    config_path = tmp_path / '.config/db/db-spec.yml'
    config_path.write_text(yml_config)

    # Create the data. Note missing values and poorly formatted strings,
    # which should be handled by init-db
    dfs = {}
    institutions = {
        'ror_id': ['02der9h97', '021sy4w91', None],
        'name': [
            None,
            '\n\tThe Jackson Laboratory for Mammalian Genetics ',
            'The Jackson Laboratory for Genomic Medicine',
        ],
        'short_name': [None, '\n\tJAX-MG ', 'JAX-GM'],
        'country': [None, None, None],
        'state': [None, None, 'CT'],
        'city': [None, None, '\n\tFarmington'],
    }
    dfs['institution.csv'] = pd.DataFrame(institutions)

    labs = {
        'pi_first_name': ['Ahmed', 'John'],
        'pi_last_name': ['Said', 'Doe'],
        'pi_email': ['ahmed.said@jax.org', 'john.doe@jax.org'],
        'pi_orcid': ['0009-0008-3754-6150', None],
        'institution_name': [
            'The Jackson Laboratory for Genomic Medicine',
            'The Jackson Laboratory for Mammalian Genetics',
        ],
        'name': [None, '\n\tService Lab '],
        'delivery_dir': [None, str(delivery_parent_dir / 'service_lab')],
    }
    dfs['lab.csv'] = pd.DataFrame(labs)

    library_types = [
        '\n\tAntibody Capture ',
        'Chromatin Accessibility',
        'CRISPR Guide Capture',
        'CytAssist Gene Expression',
        'Gene Expression',
        'Immune Profiling',
        'Multiplexing Capture',
        'Spatial Gene Expression',
    ]
    library_types = pd.DataFrame({'name': library_types})

    people = {
        'first_name': ['\n\tahmed ', 'john', 'jane'],
        'last_name': ['\n\tsaid ', 'doe', 'doe'],
        'email': ['\n\tahmed.said@jax.org ', 'john.doe@jax.org', 'jane.doe@jax.org'],
        'orcid': ['\n\t0009-0008-3754-6150 ', None, None],
    }
    dfs['person.csv'] = pd.DataFrame(people)

    # TODO: can we use the canonical 10x names for these platforms?
    platforms = [
        "\n\t3' RNA ",
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

    dfs['tag.csv'] = pd.read_csv(
        'https://raw.githubusercontent.com/TheJacksonLaboratory/nf-tenx/main/assets/tags.csv'
    )

    data_dir = tmp_path / 'data'
    for filename, df in dfs.items():
        df.to_csv(data_dir / filename, index=False)

    return str(data_dir)
