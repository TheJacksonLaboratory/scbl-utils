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
SIBLING_REPOSITORY = 'https://github.com/TheJacksonLaboratory/nf-tenx'


def n_cells_agg(n_cells_series: Series):
    comma_removed = n_cells_series.str.replace(',', '')
    as_num = to_numeric(comma_removed, errors='coerce')
    return as_num.max()


AGG_FUNCS = {
    'sample_name': 'first',
    'libraries': tuple,
    'library_types': tuple,
    '10x_platform': tuple,
    'n_cells': n_cells_agg,
    'project': 'first',
    'is_nuclei': all,
    'fastq_paths': tuple,
    'project': 'first',
    'slide': 'first',
    'area': 'first',
}
SAMPLESHEET_KEYS = ('libraries', 'library_types', 'sample_name', 'n_cells', 'is_nuclei', 'tool', 'tool_version', 'command', 'fastq_paths', 'reference_path', 'use_undetermined', 'lanes', 'design', 'probe_set', 'tags', 'no_bam', 'slide', 'area', 'image', 'roi_json', 'cyta_image', 'manual_alignment')
_ref_parent_dir = Path('/sc/service/pipelines/references')
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
PLATFORMS_TO_PROBESET = {
    'Flex': {
        'GRCh38-2020-A': '1.0/Chromium_Human_Transcriptome_Probe_Set_v1.0.1_GRCh38-2020-A.csv',
        'mm10-2020-A': '1.0/Chromium_Mouse_Transcriptome_Probe_Set_v1.0.1_mm10-2020-A.csv',
    },
    'Visium CytAssist FFPE': {
        'GRCh38-2020-A': '2.0/Visium_Human_Transcriptome_Probe_Set_v2.0_GRCh38-2020-A.csv',
        'mm10-2020-A': '1.3.0/Visium_Mouse_Transcriptome_Probe_Set_v1.0_mm10-2020-A.csv',
    },
    'Visium FF': {
        'GRCh38-2020-A': '2.0/Visium_Human_Transcriptome_Probe_Set_v2.0_GRCh38-2020-A.csv',
        'mm10-2020-A': '1.3.0/Visium_Mouse_Transcriptome_Probe_Set_v1.0_mm10-2020-A.csv',
    },
    'Visium FFPE': {
        'GRCh38-2020-A': '2.0/Visium_Human_Transcriptome_Probe_Set_v2.0_GRCh38-2020-A.csv',
        'mm10-2020-A': '1.3.0/Visium_Mouse_Transcriptome_Probe_Set_v1.0_mm10-2020-A.csv',
    },
}
VISIUM_DIR = Path('/sc/service/imaging/visium')
TRACKING_DF_INDEX_COL = 'libraries'