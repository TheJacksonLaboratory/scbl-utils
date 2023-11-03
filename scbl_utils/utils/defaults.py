from pathlib import Path
from string import ascii_letters, digits

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
        'metricssheet-spec.yml',
        'trackingsheet-spec.yml',
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
    'reference_path': list[str],
    'use_undetermined': bool,
    'lanes': str,
    'design': dict[str, dict[str, str]],
    'probe_set': list[str],
    'tags': list[str],
    'no_bam': bool,
}
REF_PARENT_DIR = Path('/sc/service/pipelines/references')
LIB_TYPES_TO_PROGRAM = {
    ('Chromatin Accessibility',): ('cellranger-atac', 'count', ['10x-atac']),
    ('CytAssist Gene Expression',): ('spaceranger', 'count', ['10x-vis']),
    ('Gene Expression',): ('cellranger', 'count', ['10x-rna']),
    ('Immune Profiling',): ('cellranger', 'vdj', ['10x-vdj']),
    ('Spatial Gene Expression',): ('spaceranger', 'count', ['10x-vis']),
    ('Antibody Capture', 'Gene Expression'): ('cellranger', 'count', ['10x-rna']),
    ('CRISPR Guide Capture', 'Gene Expression'): ('cellranger', 'count', ['10x-rna']),
    ('Chromatin Accessibility', 'Gene Expression'): (
        'cellranger-arc',
        'count',
        ['10x-arc'],
    ),
    ('Gene Expression', 'Multiplexing Capture'): ('cellranger', 'multi', ['10x-rna']),
    ('Gene Expression', 'Immune Profiling'): (
        'cellranger',
        'multi',
        ['10x-rna', '10x-vdj'],
    ),
    ('Antibody Capture', 'Gene Expression', 'Immune Profiling'): (
        'cellranger',
        'multi',
        ['10x-rna', '10x-vdj'],
    ),
    (
        'Antibody Capture',
        'CRISPR Guide Capture',
        'Gene Expression',
        'Immune Profiling',
    ): ('cellranger', 'multi', ['10x-rna', '10x-vdj']),
}
REF_DIRS = {
    Path(ref_dir)
    for tool_command_refdirs in LIB_TYPES_TO_PROGRAM.values()
    for ref_dir in tool_command_refdirs[2]
}
SAMPLENAME_BLACKLIST_PATTERN = f'[^{ascii_letters + digits}]'
