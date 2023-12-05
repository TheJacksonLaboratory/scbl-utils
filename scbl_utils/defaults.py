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
LIBRARY_ID_PATTERN = r'^SC\d{7}$'
PROJECT_ID_PATTERN = r'^SCP\d{2}-\d{3}$'
# DELIVERY_PARENT_DIR = '/sc/service/delivery'

# Samplesheet formatting settings
SEP_CHARS = r'\s_-'
SEP_PATTERN = rf'[{SEP_CHARS}]'
SAMPLENAME_BLACKLIST_PATTERN = rf'[^{ascii_letters + digits + SEP_CHARS}]'

# Configuration files necesary for database connection
DB_CONFIG_FILES = [Path(filename) for filename in ('db-spec.yml',)]

# JSON schema for configuration file
_schema_draft_version = 'https://json-schema.org/draft/2020-12/schema'
_db_drivers = ['sqlite']
SPEC_SCHEMA = {
    '$schema': _schema_draft_version,
    'type': 'object',
    'properties': {
        'database': {'type': 'string'},
        'drivername': {'type': 'string', 'enum': _db_drivers},
    },
    'required': ['database', 'drivername'],
    'additionalProperties': False,
}

# JSON schema for CSV files
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
            'pi_email': {'type': _string_or_null, 'format': 'email'},
            'pi_first_name': {'type': _string_or_null},
            'pi_last_name': {'type': _string_or_null},
            'institution_short_name': {'type': 'string'},
            'delivery_dir': {'type': _string_or_null},
            'name': {'type': _string_or_null},
            'pi_orcid': {'type': _string_or_null, 'pattern': ORCID_PATTERN},
        },
        'required': [
            'pi_email',
            'pi_first_name',
            'pi_last_name',
            'pi_orcid',
            'institution_short_name',
            'delivery_dir',
            'name',
        ],
        'additionalProperties': False,
    },
}
