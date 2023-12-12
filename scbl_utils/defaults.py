"""
This module contains some handy defaults for the `scbl-utils` package.
"""
from itertools import zip_longest
from pathlib import Path
from string import ascii_letters, digits

from rich.console import Console
from rich.table import Table
from typer import Abort

# Package metadata
DOCUMENTATION = 'https://github.com/TheJacksonLaboratory/scbl-utils/'
SEE_MORE = f'See {DOCUMENTATION} for more information.'
SIBLING_REPOSITORY = 'https://github.com/TheJacksonLaboratory/nf-tenx'

# SCBL-specific settings
CONFIG_DIR = Path('/sc/service/etc/.config/scbl-utils')
LIBRARY_GLOB_PATTERN = 'SC*fastq*'
LIBRARY_ID_PATTERN = r'^SC\d{7}$'
PROJECT_ID_PATTERN = r'^SCP\d{2}-\d{3}$'

# Samplesheet formatting settings
SEP_CHARS = r'\s_-'
SEP_PATTERN = rf'[{SEP_CHARS}]'
SAMPLENAME_BLACKLIST_PATTERN = rf'[^{ascii_letters + digits + SEP_CHARS}]'

# Configuration files necesary for script
DB_CONFIG_FILES = [Path(filename) for filename in ('db-spec.yml',)]
GDRIVE_CONFIG_FILES = [
    Path(filename) for filename in ('gdrive-spec.yml', 'service-account.json')
]

# CSV files necessary for database initialization
DB_INIT_FILES = {
    f'{table_name}.csv': Path(f'{table_name}.csv')
    for table_name in (
        'institution',
        'lab',
        'librarytype',
        'person',
        'platform',
        'tag',
    )
}

# JSON schema for configuration file
_schema_draft_version = 'https://json-schema.org/draft/2020-12/schema'
_db_drivers = ['sqlite']
SPEC_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'object',
    'properties': {
        'database': {'type': ['string', 'null']},
        'drivername': {'type': 'string', 'enum': _db_drivers},
    },
    'required': ['database', 'drivername'],
    'additionalProperties': False,
}

# TODO: should these schema be replaced by auto-generated schema using
# sqlmodel, a wrapper of sqlalchemy and pydantic?

# JSON schemas for CSV files
_string_or_null = ['string', 'null']
INSTITUION_CSV_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'name': {'type': _string_or_null},
            'short_name': {'type': _string_or_null},
            'country': {'type': _string_or_null, 'minLength': 2, 'maxLength': 2},
            'state': {'type': _string_or_null, 'minLength': 2, 'maxLength': 2},
            'city': {'type': _string_or_null},
            'ror_id': {'type': _string_or_null},
        },
        'required': ['name', 'short_name', 'country', 'state', 'city', 'ror_id'],
        'additionalProperties': False,
    },
}

ORCID_PATTERN = r'^(\d{4})-?(\d{4})-?(\d{4})-?(\d{4}|\d{3}X)$'
LAB_CSV_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'institution_name': {'type': 'string'},
            'pi_email': {'type': _string_or_null, 'format': 'email'},
            'pi_first_name': {'type': 'string'},
            'pi_last_name': {'type': 'string'},
            'pi_orcid': {'type': _string_or_null, 'pattern': ORCID_PATTERN},
            'delivery_dir': {'type': _string_or_null},
            'name': {'type': _string_or_null},
        },
        'required': [
            'pi_email',
            'pi_first_name',
            'pi_last_name',
            'pi_orcid',
            'institution_name',
            'delivery_dir',
            'name',
        ],
        'additionalProperties': False,
    },
}

PERSON_CSV_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'first_name': {'type': 'string'},
            'last_name': {'type': 'string'},
            'email': {'type': _string_or_null, 'format': 'email'},
            'orcid': {'type': _string_or_null, 'pattern': ORCID_PATTERN},
        },
        'required': ['first_name', 'last_name', 'email', 'orcid'],
        'additionalProperties': False,
    },
}

# TODO: eventually update this
PLATFORM_CSV_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
        },
        'required': ['name'],
        'additionalProperties': False,
    },
}

# TODO eventually update this
LIBRARY_TYPE_CSV_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'name': {'type': 'string'},
        },
        'required': ['name'],
        'additionalProperties': False,
    },
}

# TODO: eventually update this
TAG_CSV_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},  # TODO: add a pattern to this?
            'name': {'type': ['string', 'null']},
            'five_prime_offset': {'type': 'integer'},
            'tag_type': {'type': 'string'},
            'read': {'type': 'string', 'pattern': r'^R\d$'},
            'sequence': {'type': 'string', 'pattern': r'^[ACGTN]*$'},
            'pattern': {'type': 'string', 'pattern': r'^[35]P[ACTGN]*\(BC\)$'},
        },
        'required': [
            'id',
            'name',
            'five_prime_offset',
            'tag_type',
            'read',
            'sequence',
            'pattern',
        ],
        'additionalProperties': False,
    },
}

CSV_SCHEMAS = {
    'institution.csv': INSTITUION_CSV_SCHEMA,
    'lab.csv': LAB_CSV_SCHEMA,
    'person.csv': PERSON_CSV_SCHEMA,
    'platform.csv': PLATFORM_CSV_SCHEMA,
    'librarytype.csv': LIBRARY_TYPE_CSV_SCHEMA,
    'tag.csv': TAG_CSV_SCHEMA,
}

if DB_INIT_FILES.keys() != CSV_SCHEMAS.keys():
    mismatch_table = Table('CSV Schemas', 'Database Initialization Files')
    for row in zip_longest(CSV_SCHEMAS.keys(), DB_INIT_FILES.keys(), fillvalue=''):
        mismatch_table.add_row(*row)

    console = Console()
    console.print(
        'The CSV schemas ([green]CSV_SCHEMAS[/]) and the required database '
        'initialization files ([green]DB_INIT_FILES[/]) must have the same '
        f'keys. All are defined in {__file__}.',
        mismatch_table,
        sep='\n',
    )

    raise Abort()
