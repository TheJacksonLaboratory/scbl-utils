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
    'Institution',
    'Person',
    'Lab',
    'Project',
    'Platform',
    'LibraryType',
    'Tag',
    'SequencingRun',
    'ChromiumDataSet',
    'Library',
    'ChromiumSample',
    'XeniumRun',
    'XeniumDataSet',
    'XeniumSample',
)
EMAIL_FORMAT_VARIABLE_PATTERN = r'{[^{}]+}'

# Configuration files necesary for script
# TODO: not every file is required for every task
DB_CONFIG_FILES = [Path(filename) for filename in ('db-spec.yml',)]
# TODO: this can probably be made dynamic. We can store in the database
# a list of assay types or tracking sheets, then the script can query
# that against the configuration directory. Can also enforce a structure
# of a tracking-sheet directory that lives alongside
# service-account.json
GDRIVE_CONFIG_FILES = [
    Path(filename)
    for filename in ('service-account.json', 'platform_tracking-sheet_specs')
]

# CSV files necessary for database initialization
# This isn't even necessary
DB_INIT_FILES = [
    Path(f'{model_name}.csv')
    for model_name in ('Institution', 'Lab', 'Person', 'Platform', 'LibraryType', 'Tag')
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
GDRIVE_PLATFORM_SPEC_SCHEMA = {
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
            'patternProperties': {
                r'^\d+$': {
                    'type': 'object',
                    'properties': {
                        'replace': {'type': 'object'},
                        'head': {'type': 'integer'},
                        'type_converters': {'type': 'object'},
                        'index_col': {'type': 'string'},
                        'empty_means_drop': {
                            'type': 'array',
                            'items': {'type': 'string'},
                        },
                        'cols_to_targets': {
                            'type': 'array',
                            'items': {
                                'type': 'object',
                                'properties': {
                                    'from': {'type': 'string'},
                                    'to': {
                                        'type': 'array',
                                        'items': {'type': 'string'},
                                    },
                                    'mapper': {'type': 'object'},
                                },
                            },
                        },
                    },
                    'required': [
                        'index_col',
                        'empty_means_drop',
                        'replace',
                        'cols_to_targets',
                        'head',
                        'type_converters',
                    ],
                    'additionalProperties': False,
                },
            },
        },
    },
}

# All CSVs follow same skeleton of a schema, so just define the
# properties
_institution_properties = {
    'Institution.name': {'type': _string_or_null},
    'Institution.short_name': {'type': _string_or_null},
    'Institution.email_format': {'type': 'string'},
    'Institution.country': {
        'type': _string_or_null,
        'minLength': 2,
        'maxLength': 2,
    },
    'Institution.state': {
        'type': _string_or_null,
        'minLength': 2,
        'maxLength': 2,
    },
    'Institution.city': {'type': _string_or_null},
    'Institution.ror_id': {'type': _string_or_null},
}

ORCID_PATTERN = r'^(\d{4})-?(\d{4})-?(\d{4})-?(\d{4}|\d{3}X)$'
_lab_properties = {
    'Lab.institution.name': {'type': 'string'},
    'Lab.pi.email': {'type': _string_or_null, 'format': 'email'},
    'Lab.pi.first_name': {'type': 'string'},
    'Lab.pi.last_name': {'type': 'string'},
    'Lab.pi.orcid': {'type': _string_or_null, 'pattern': ORCID_PATTERN},
    'Lab.delivery_dir': {'type': _string_or_null},
    'Lab.name': {'type': _string_or_null},
}

_person_properties = {
    'Person.first_name': {'type': 'string'},
    'Person.last_name': {'type': 'string'},
    'Person.email': {'type': _string_or_null, 'format': 'email'},
    'Person.orcid': {'type': _string_or_null, 'pattern': ORCID_PATTERN},
    'Person.institution.name': {'type': 'string'},
}


_platform_properties = {'Platform.name': {'type': 'string'}}

_library_type_properties = {'LibraryType.name': {'type': 'string'}}

_tag_properties = {
    'Tag.id': {'type': 'string'},
    'Tag.name': {'type': _string_or_null},
    'Tag.five_prime_offset': {'type': 'integer'},
    'Tag.type': {'type': 'string'},
    'Tag.read': {'type': 'string', 'pattern': r'^R\d$'},
    'Tag.sequence': {'type': 'string', 'pattern': r'^[ACGTN]*$'},
    'Tag.pattern': {'type': 'string', 'pattern': r'^[35]P[ACTGN]*\(BC\)$'},
}

# Construct the full schema from the properties
_properties: dict[str, dict] = {
    'Institution': _institution_properties,
    'Lab': _lab_properties,
    'Person': _person_properties,
    'Platform': _platform_properties,
    'LibraryType': _library_type_properties,
    'Tag': _tag_properties,
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
