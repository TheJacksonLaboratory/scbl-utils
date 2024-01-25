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

# Handy variables for JSON schema
_schema_draft_version = 'https://json-schema.org/draft/2020-12/schema'
_string_or_null = ['string', 'null']

DB_SPEC_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'object',
    'properties': {
        'database': {'type': _string_or_null},
        'drivername': {'type': 'string', 'enum': ['sqlite']},
    },
    'required': ['database', 'drivername'],
    'additionalProperties': False,
}

# JSON schema for Google Drive configuration file
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

# All CSVs follow same skeleton of a schema, so just define the
# properties
_institution_properties = {
    'institution.name': {'type': _string_or_null},
    'institution.short_name': {'type': _string_or_null},
    'institution.email_format': {'type': 'string'},
    'institution.country': {
        'type': _string_or_null,
        'minLength': 2,
        'maxLength': 2,
    },
    'institution.state': {
        'type': _string_or_null,
        'minLength': 2,
        'maxLength': 2,
    },
    'institution.city': {'type': _string_or_null},
    'institution.ror_id': {'type': _string_or_null},
}

ORCID_PATTERN = r'^(\d{4})-?(\d{4})-?(\d{4})-?(\d{4}|\d{3}X)$'
_lab_properties = {
    'lab.institution.name': {'type': 'string'},
    'lab.pi.email': {'type': _string_or_null, 'format': 'email'},
    'lab.pi.first_name': {'type': 'string'},
    'lab.pi.last_name': {'type': 'string'},
    'lab.pi.orcid': {'type': _string_or_null, 'pattern': ORCID_PATTERN},
    'lab.delivery_dir': {'type': _string_or_null},
    'lab.name': {'type': _string_or_null},
}

_person_properties = {
    'person.first_name': {'type': 'string'},
    'person.last_name': {'type': 'string'},
    'person.email': {'type': _string_or_null, 'format': 'email'},
    'person.orcid': {'type': _string_or_null, 'pattern': ORCID_PATTERN},
    'person.institution.name': {'type': 'string'},
}

_platform_properties = {'platform.name': {'type': 'string'}}

_library_type_properties = {'library_type.name': {'type': 'string'}}

_tag_properties = {
    'tag.id': {'type': 'string'},
    'tag.name': {'type': _string_or_null},
    'tag.five_prime_offset': {'type': 'integer'},
    'tag.tag_type': {'type': 'string'},
    'tag.read': {'type': 'string', 'pattern': r'^R\d$'},
    'tag.sequence': {'type': 'string', 'pattern': r'^[ACGTN]*$'},
    'tag.pattern': {'type': 'string', 'pattern': r'^[35]P[ACTGN]*\(BC\)$'},
}

# Construct the full schema from the properties
_properties: dict[str, dict] = {
    'institution': _institution_properties,
    'lab': _lab_properties,
    'person': _person_properties,
    'platform': _platform_properties,
    'library_type': _library_type_properties,
    'tag': _tag_properties,
}
DATA_SCHEMAS = {
    model: {
        '$schema': _schema_draft_version,
        'type': 'array',
        'items': {
            'type': 'object',
            'properties': model_properties,
            'required': list(model_properties.keys()),
            'additionalProperties': False,
        },
    }
    for model, model_properties in _properties.items()
}
