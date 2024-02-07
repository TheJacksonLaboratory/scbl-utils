# Handy variables for JSON schema
_schema_draft_version = 'https://json-schema.org/draft/2020-12/schema'
_string_or_null = ['string', 'null']

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
    'Institution.csv': _institution_properties,
    'Lab.csv': _lab_properties,
    'Person.csv': _person_properties,
    'Platform.csv': _platform_properties,
    'LibraryType.csv': _library_type_properties,
    'Tag.csv': _tag_properties,
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
