"""
This module contains some handy defaults for the `scbl-utils` package.
"""
from pathlib import Path
from string import ascii_letters, digits

# Package metadata
DOCUMENTATION = 'https://github.com/TheJacksonLaboratory/scbl-utils/'
SEE_MORE = f'See {DOCUMENTATION} for more information.'
SIBLING_REPOSITORY = 'https://github.com/TheJacksonLaboratory/nf-tenx'

# SCBL-specific settings
CONFIG_DIR = Path('/sc/service/etc/.config/scbl-utils')
LIBRARY_GLOB_PATTERN = 'SC*fastq*'
LIBRARY_ID_PATTERN = r'^SC\d{7}\w?$'
PROJECT_ID_PATTERN = r'^SCP\d{2}-\d{3}$'

# Samplesheet formatting settings
SEP_CHARS = r'\s_-'
SEP_PATTERN = rf'[{SEP_CHARS}]'
SAMPLENAME_BLACKLIST_PATTERN = rf'[^{ascii_letters + digits + SEP_CHARS}]'

# TODO: write a test to make sure that these are all the tables and that the email_format_variable_chars are just two chars
DATA_INSERTION_ORDER = (
    'institution',
    'person',
    'lab',
    'project',
    'platform',
    'library_type',
    'tag',
    'sequencing_run',
    'data_set',
    'library',
    'sample',
)
LEFT_FORMAT_CHAR, RIGHT_FORMAT_CHAR = r'{', r'}'
EMAIL_FORMAT_VARIABLE_PATTERN = r'{[^{}]+}'

# Configuration files necesary for script
DB_CONFIG_FILES = [Path(filename) for filename in ('db-spec.yml',)]
GDRIVE_CONFIG_FILES = [
    Path(filename) for filename in ('gdrive-spec.yml', 'service-account.json')
]

# CSV files necessary for database initialization
# This isn't even necessary
DB_INIT_FILES = [
    Path(f'{table_name}.csv')
    for table_name in (
        'institution',
        'lab',
        'library_type',
        'person',
        'platform',
        'tag',
    )
]
OBJECT_SEP_CHAR = '.'
OBJECT_SEP_PATTERN = r'\.'

# JSON schema for db configuration file
_schema_draft_version = 'https://json-schema.org/draft/2020-12/schema'
_db_drivers = ['sqlite']
DB_SPEC_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'object',
    'properties': {
        'database': {'type': ['string', 'null']},
        'drivername': {'type': 'string', 'enum': _db_drivers},
    },
    'required': ['database', 'drivername'],
    'additionalProperties': False,
}

# JSON schema for Google Drive configuration file
# TODO: fix this
GDRIVE_SPEC_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'object',
    'properties': {
        'spreadsheet_url': {
            'type': 'string',
            'pattern': r'^https://docs.google.com/spreadsheets/d/.*$',
        },
        'main_sheet_id': {'type': 'string', 'pattern': r'^\d+$'},
        'worksheets': {
            'type': 'object',
            'properties': {
                'replace': {'type': 'object'},
                'index_col': {'type': 'string'},
                'empty_means_drop': {'type': 'array', 'items': {'type': 'string'}},
                'cols_to_targets': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'from': {'type': 'string'},
                            'to': {'type': 'array', 'items': {'type': 'string'}},
                            'mapper': {'type': 'object'},
                        },
                    },
                    'type_converters': {'type': 'object'},
                    'head': {'type': 'integer'},
                },
            },
            'required': ['index_col', 'empty_means_drop', 'head'],
            'additionalProperties': ['replace', 'cols_to_targets', 'type_converters'],
        },
    },
}

# JSON schemas for CSV files
_string_or_null = ['string', 'null']
INSTITUION_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'name': {'type': _string_or_null},
            'short_name': {'type': _string_or_null},
            'email_format': {'type': 'string'},
            'country': {'type': _string_or_null, 'minLength': 2, 'maxLength': 2},
            'state': {'type': _string_or_null, 'minLength': 2, 'maxLength': 2},
            'city': {'type': _string_or_null},
            'ror_id': {'type': _string_or_null},
        },
        'required': [
            'name',
            'short_name',
            'email_format',
            'country',
            'state',
            'city',
            'ror_id',
        ],
        'additionalProperties': False,
    },
}

ORCID_PATTERN = r'^(\d{4})-?(\d{4})-?(\d{4})-?(\d{4}|\d{3}X)$'
LAB_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'lab.institution.name': {'type': 'string'},
            'lab.pi.email': {'type': _string_or_null, 'format': 'email'},
            'lab.pi.first_name': {'type': 'string'},
            'lab.pi.last_name': {'type': 'string'},
            'lab.pi.orcid': {'type': _string_or_null, 'pattern': ORCID_PATTERN},
            'lab.delivery_dir': {'type': _string_or_null},
            'lab.name': {'type': _string_or_null},
        },
        'required': [
            'lab.pi.email',
            'lab.pi.first_name',
            'lab.pi.last_name',
            'lab.pi.orcid',
            'lab.institution.name',
            'lab.delivery_dir',
            'lab.name',
        ],
        'additionalProperties': False,
    },
}

PERSON_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'array',
    'items': {
        'type': 'object',
        'properties': {
            'person.first_name': {'type': 'string'},
            'person.last_name': {'type': 'string'},
            'person.email': {'type': _string_or_null, 'format': 'email'},
            'person.orcid': {'type': _string_or_null, 'pattern': ORCID_PATTERN},
            'person.institution.name': {'type': 'string'},
        },
        'required': [
            'person.first_name',
            'person.last_name',
            'person.email',
            'person.orcid',
        ],
        'additionalProperties': False,
    },
}

# TODO: eventually update this
PLATFORM_SCHEMA = {
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
LIBRARY_TYPE_SCHEMA = {
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
TAG_SCHEMA = {
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

DATA_SCHEMAS = {
    'institution': INSTITUION_SCHEMA,
    'lab': LAB_SCHEMA,
    'person': PERSON_SCHEMA,
    'platform': PLATFORM_SCHEMA,
    'library_type': LIBRARY_TYPE_SCHEMA,
    'tag': TAG_SCHEMA,
}
