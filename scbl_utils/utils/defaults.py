from itertools import permutations
from pathlib import Path
from string import ascii_letters, digits

from pandas import Series, to_numeric

DOCUMENTATION = 'https://github.com/TheJacksonLaboratory/scbl-utils/'
CONFIG_DIR = Path('/sc/service/etc/.config/scbl-utils')
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]
LIBRARY_GLOB_PATTERN = 'SC*fastq*'
LIBRARY_ID_PATTERN = r'SC\d{7}'
GDRIVE_CONFIG_FILES = [
    Path(filename)
    for filename in (
        'metricssheet-spec.yml',
        'trackingsheet-spec.yml',
        'service-account.json',
    )
]
TRACKING_DF_INDEX_COL = 'libraries'
_schema_draft_version = 'https://json-schema.org/draft/2020-12/schema'
_lib_types_to_program = {
    ('Chromatin Accessibility',):
    ('cellranger-atac', 'count', ['10x-atac']),

    ('CytAssist Gene Expression',):
    ('spaceranger', 'count', ['10x-vis']),

    ('Gene Expression',):
    ('cellranger', 'count', ['10x-rna']),

    ('Immune Profiling',):
    ('cellranger', 'vdj', ['10x-vdj']),

    ('Spatial Gene Expression',):
    ('spaceranger', 'count', ['10x-vis']),

    ('Antibody Capture', 'Gene Expression'):
    ('cellranger', 'count', ['10x-rna']),

    ('CRISPR Guide Capture', 'Gene Expression'):
    ('cellranger', 'count', ['10x-rna']),

    ('Chromatin Accessibility', 'Gene Expression'):
    ('cellranger-arc', 'count', ['10x-arc']),

    ('Gene Expression', 'Multiplexing Capture'):
    ('cellranger', 'multi', ['10x-rna']),

    ('Gene Expression', 'Immune Profiling'):
    ('cellranger', 'multi', ['10x-rna', '10x-vdj']),

    ('Antibody Capture', 'Gene Expression', 'Immune Profiling'):
    ('cellranger', 'multi', ['10x-rna', '10x-vdj']),

    ('Antibody Capture', 'CRISPR Guide Capture', 'Gene Expression', 'Immune Profiling'):
    ('cellranger', 'multi', ['10x-rna', '10x-vdj']),
}
LIB_TYPES = {lib_type for lib_tuple in _lib_types_to_program for lib_type in lib_tuple}
SPEC_SCHEMA = {
    'trackingsheet-spec.yml': {
        '$schema': _schema_draft_version,
        'type': 'object',
        'properties': {
            'id': {'type': 'string'},
            'sheets': {
                'type': 'object',
                'patternProperties': {
                    '.+': {
                        'type': 'object',
                        'properties': {
                            'columns': {
                                'type': 'object',
                                'patternProperties': {
                                    '.+': {
                                        'type': 'string',
                                    }
                                },
                            },
                            'header_row': {'type': 'integer'},
                            'join': {'type': 'boolean'},
                        },
                        'required': ['columns', 'header_row', 'join'],
                    }
                },
            },
            'platform_to_lib_type': {
                'type': 'object',
                'patternProperties': {
                    '.+': {'type': 'string', 'enum': list(LIB_TYPES)}
                },
            },
        },
        'required': ['id', 'sheets', 'platform_to_lib_type'],
    },
    'metricssheet-spec.yml': {
        '$schema': _schema_draft_version,
        'type': 'object',
        'properties': {
            'dir_id': {'type': 'string'},
            'header_row': {'type': 'integer'},
            'columns': {
                'type': 'object',
                'patternProperties': {'.+': {'type': 'string'}},
            },
        },
        'required': ['dir_id', 'header_row', 'columns'],
    },
}
REQUIRED_METRICSSHEET_SPEC_KEYS = {
    'project',
    'tool',
    'tool_version',
    'reference',
    'libraries',
}
REQUIRED_TRACKINGSHEET_SPEC_COLUMNS = {
    'libraries',
    'sample_name',
    'is_nuclei',
    'project',
    'species',
    'n_cells',
    'slide',
    'area',
    'sub_sample_name',
    'tag_id',
    'description',
}
SIBLING_REPOSITORY = 'https://github.com/TheJacksonLaboratory/nf-tenx'


def n_cells_agg(n_cells_series: Series):
    comma_removed = n_cells_series.str.replace(',', '')
    as_num = to_numeric(comma_removed, errors='coerce')
    return as_num.max()


def design_agg(designs: Series):
    overall_design = {}
    libs_with_design = designs.dropna()
    for design in libs_with_design:
        overall_design |= design
    return overall_design


AGG_FUNCS = {
    'sample_name': 'first',
    'date_submitted': 'first',
    'pi': 'first',
    'submitter_name': 'first',
    'libraries': tuple,
    'library_types': tuple,
    '10x_platform': tuple,
    'n_cells': n_cells_agg,
    'project': 'first',
    'is_nuclei': all,
    'species': 'first',
    'design': design_agg,
    'fastq_paths': tuple,
    'project': 'first',
    'slide': 'first',
    'area': 'first',
}
SAMPLESHEET_KEYS = (
    'libraries',
    'library_types',
    'sample_name',
    'n_cells',
    'is_nuclei',
    'tool',
    'tool_version',
    'command',
    'fastq_paths',
    'reference_path',
    'use_undetermined',
    'lanes',
    'design',
    'probe_set',
    'tags',
    'no_bam',
    'slide',
    'area',
    'image',
    'roi_json',
    'cyta_image',
    'manual_alignment',
)
_ref_parent_dir = Path('/sc/service/pipelines/references')
_lib_types_to_program = {
    key: (tool, command, [_ref_parent_dir / path for path in ref_dirs])
    for key, (tool, command, ref_dirs) in _lib_types_to_program.items()
}
LIB_TYPES_TO_PROGRAM = {
    permutation: Series(program)
    for lib_combo, program in _lib_types_to_program.items()
    for permutation in permutations(lib_combo, len(lib_combo))
}
REF_DIRS = {
    Path(ref_dir)
    for tool_command_refdirs in LIB_TYPES_TO_PROGRAM.values()
    for ref_dir in tool_command_refdirs[2]
}
SEP_CHARS = r'\s_-'
SAMPLENAME_BLACKLIST_PATTERN = rf'[^{ascii_letters + digits + SEP_CHARS}]'
SEP_PATTERN = rf'[{SEP_CHARS}]'
SAMPLESHEET_SORT_KEYS = ['library_types', 'sample_name']
SAMPLESHEET_GROUP_KEY = 'library_types'
ANTIBODY_LIB_TYPES = {'Antibody Capture'}
_species_to_flex_probesets = {
    'H sapiens': '1.0/Chromium_Human_Transcriptome_Probe_Set_v1.0.1_GRCh38-2020-A.csv',
    'M musculus': '1.0/Chromium_Mouse_Transcriptome_Probe_Set_v1.0.1_mm10-2020-A.csv',
}
_species_to_visium_probesets = {
    'H sapiens': '2.0/Visium_Human_Transcriptome_Probe_Set_v2.0_GRCh38-2020-A.csv',
    'M musculus': '1.3.0/Visium_Mouse_Transcriptome_Probe_Set_v1.0_mm10-2020-A.csv',
}
PLATFORMS_TO_PROBESET = {
    'Flex': _species_to_flex_probesets,
    'Visium CytAssist FFPE': _species_to_visium_probesets,
    'Visium FF': _species_to_visium_probesets,
    'Visium FFPE': _species_to_visium_probesets,
}
VISIUM_DIR = Path('/sc/service/imaging/visium')
_species_to_genomes = {
    'M musculus': (r'mm\d*', r'GRCm\d*'),
    'H sapiens': (r'hg\d*', r'GRCh\d*'),
    'G aculeatus': (r'GAculeatus',),
}
SPECIES_TO_GENOME_PATTERN = {
    species: rf'^(?!.*and).*({"|".join(genomes)}).*$'
    for species, genomes in _species_to_genomes.items()
}
_human_mouse_genomes = '|'.join(
    _species_to_genomes['H sapiens'] + _species_to_genomes['M musculus']
)
_human_mouse_pattern = rf'^.*({_human_mouse_genomes}).*({_human_mouse_genomes}).*$'
SPECIES_TO_GENOME_PATTERN['H sapiens + M musculus'] = _human_mouse_pattern
SPECIES_TO_GENOME_PATTERN['M musculus + H sapiens'] = _human_mouse_pattern
