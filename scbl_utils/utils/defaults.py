from pathlib import Path

toml = (Path(__package__).parent / 'pyproject.toml').read_text()

try:
    from tomllib import loads

    toml = loads(toml)
    DOCUMENTATION = toml['tool']['poetry']['documentation']
except ImportError:
    from re import search

    DOCUMENTATION = search(pattern=r'documentation = "(.*)"\n', string=toml).group(1)  # type: ignore

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
    'fastq_paths': list[str],
    'tool': str,
    'tool_version': str,
    'command': str,
    'sample_name': str,
    'reference_path': str,
    'use_undetermined': bool,
    'lanes': str,
    'n_cells': int,
    'is_nuclei': bool,
    'design': dict[str, str],
    'probe_set': list[str],
    'tags': list[str],
    'no_bam': bool,
}
LIB_TYPES_TO_PROGRAM = [
    {
        'library_types': ['Chromatin Accessibility', 'Gene Expression'],
        'tool': 'cellranger-arc',
        'command': 'count',
        'reference_dir': '/sc/service/pipelines/references/10x-arc',
    },
    {
        'library_types': ['Gene Expression'],
        'tool': 'cellranger',
        'command': 'count',
        'reference_dir': '/sc/service/pipelines/references/10x-rna',
    },
    {
        'library_types': ['CytAssist Gene Expression', 'Spatial Gene Expression'],
        'tool': 'spaceranger',
        'command': 'count',
        'reference_dir': '/sc/service/pipelines/references/10x-vis',
    },
    {
        'library_types': ['Antibody Capture', 'Gene Expression'],
        'tool': 'cellranger',
        'command': 'count',
        'reference_dir': '/sc/service/pipelines/references/10x-rna',
    },
    {
        'library_types': ['Chromatin Accessibility'],
        'tool': 'cellranger-atac',
        'command': 'count',
        'reference_dir': '/sc/service/pipelines/references/10x-atac',
    },
    {
        'library_types': ['Gene Expression', 'Multiplexing Capture'],
        'tool': 'cellranger',
        'command': 'multi',
        'reference_dir': '/sc/service/pipelines/references/10x-atac',
    },
]
