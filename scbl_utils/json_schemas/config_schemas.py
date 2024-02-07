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
    'required': ['spreadsheet_url', 'main_sheet_id', 'worksheets'],
    'additionalProperties': False,
}

SYSTEM_CONFIG_SCHEMA = {'$schema': _schema_draft_version, 'type': 'object', 'properties': {'delivery_parent_dir': {'type': 'string', 'format': 'uri-reference'}}, 'required': ['delivery_parent_dir'], 'additionalProperties': False}