from string import ascii_letters, digits
from pathlib import Path

DOCUMENTATION = 'https://github.com/TheJacksonLaboratory/scbl-utils/'
CONFIG_DIR = Path('/sc/service/etc/.config/scbl-utils')
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]
LIBRARY_GLOB_PATTERN = 'SC*fastq*'
GDRIVE_CONFIG_FILES = [
    Path(filename)
    for filename in (
        'metricssheet_spec.yml',
        'trackingsheet_spec.yml',
        'service-account.json',
    )
]
SIBLING_REPOSITORY = 'https://github.com/TheJacksonLaboratory/nf-tenx'
SAMPLESHEET_KEY_TO_TYPE = {
    'libraries': list[str],
    'library_types': list[str],
    'sample_name': str,
    'n_cells': int,
    'is_nuclei': bool,
    'tool': str,
    'tool_version': str,
    'command': str,
    'fastq_paths': list[str],
    'reference_path': str,
    'use_undetermined': bool,
    'lanes': str,
    'design': dict[str, dict[str, str]],
    'probe_set': list[str],
    'tags': list[str],
    'no_bam': bool,
}
REF_PARENT_DIR = Path('/sc/service/pipelines/references')
LIB_TYPES_TO_PROGRAM = [
    {
        'library_types': ['Chromatin Accessibility', 'Gene Expression'],
        'tool': 'cellranger-arc',
        'command': 'count',
        'reference_dir': '10x-arc',
    },
    {
        'library_types': ['Gene Expression'],
        'tool': 'cellranger',
        'command': 'count',
        'reference_dir': '10x-rna',
    },
    {
        'library_types': ['CytAssist Gene Expression', 'Spatial Gene Expression'],
        'tool': 'spaceranger',
        'command': 'count',
        'reference_dir': '10x-vis',
    },
    {
        'library_types': ['Antibody Capture', 'Gene Expression'],
        'tool': 'cellranger',
        'command': 'count',
        'reference_dir': '10x-rna',
    },
    {
        'library_types': ['Chromatin Accessibility'],
        'tool': 'cellranger-atac',
        'command': 'count',
        'reference_dir': '10x-atac',
    },
    {
        'library_types': ['Gene Expression', 'Multiplexing Capture'],
        'tool': 'cellranger',
        'command': 'multi',
        'reference_dir': '10x-atac',
    },
]

for lib_dict in LIB_TYPES_TO_PROGRAM:
    lib_dict['reference_dir'] = REF_PARENT_DIR / lib_dict['reference_dir']

SAMPLENAME_BLACKLIST_PATTERN = f'[^{ascii_letters + digits}]'