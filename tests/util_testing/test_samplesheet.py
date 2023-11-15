from pathlib import Path

import pandas as pd
from pytest import fixture, mark, raises
from typer import Abort

from scbl_utils.utils import samplesheet
from scbl_utils.utils.defaults import LIB_TYPES_TO_PROGRAM, SAMPLENAME_BLACKLIST_PATTERN


@fixture
def fastqdirs(dirs: dict[str, Path]) -> list[Path]:
    return list((dirs['data'] / 'fastqs').iterdir())


def test_mapping_correct(fastqdirs: list[Path]):
    correct_output = {
        'SC9900000': "/Users/saida/work/scbl-utils/tests/data/fastqs/5' VDJ Sample 0",
        'SC9900001': '/Users/saida/work/scbl-utils/tests/data/fastqs/ATAC Sample 0',
        'SC9900002': '/Users/saida/work/scbl-utils/tests/data/fastqs/ATAC v2 Sample 0',
        'SC9900003': '/Users/saida/work/scbl-utils/tests/data/fastqs/Automated RNA Sample 0',
        'SC9900004': '/Users/saida/work/scbl-utils/tests/data/fastqs/Flex Sample 0',
        'SC9900005': '/Users/saida/work/scbl-utils/tests/data/fastqs/RNA Sample 0',
        'SC9900006': '/Users/saida/work/scbl-utils/tests/data/fastqs/RNA-HT Sample 0',
        'SC9900007': '/Users/saida/work/scbl-utils/tests/data/fastqs/Visium CytAssist FFPE Sample 0',
        'SC9900008': '/Users/saida/work/scbl-utils/tests/data/fastqs/Visium FF Sample 0',
        'SC9900009': '/Users/saida/work/scbl-utils/tests/data/fastqs/Visium FFPE Sample 0',
        'SC9900010': '/Users/saida/work/scbl-utils/tests/data/fastqs/Multiome Sample 0',
        'SC9900011': '/Users/saida/work/scbl-utils/tests/data/fastqs/Multiome Sample 0',
        'SC9900012': '/Users/saida/work/scbl-utils/tests/data/fastqs/Cell Surface Sample 0',
        'SC9900013': '/Users/saida/work/scbl-utils/tests/data/fastqs/Cell Surface Sample 0',
        'SC9900014': '/Users/saida/work/scbl-utils/tests/data/fastqs/CellPlex Sample 0',
        'SC9900015': '/Users/saida/work/scbl-utils/tests/data/fastqs/CellPlex Sample 0',
    }
    assert samplesheet.map_libs_to_fastqdirs(fastqdirs) == correct_output
    assert samplesheet.map_libs_to_fastqdirs([]) == {}


def test_mapping_wrong_glob_pattern(fastqdirs: list[Path]):
    with raises(Abort):
        samplesheet.map_libs_to_fastqdirs(fastqdirs, glob_pattern='*wrong*pattern*')


@fixture
def sample_name_df(
    dirs: dict[str, Path], fastqdirs: list[Path], tracking_spec: dict, request
):
    df = pd.read_csv(dirs['data'] / 'test-trackingsheet.csv', index_col=0)
    lib_to_fastqdir = samplesheet.map_libs_to_fastqdirs(fastqdirs)

    # Fill samplesheet with available information
    df['fastq_paths'] = df.index.map(lib_to_fastqdir)
    df['library_types'] = df['10x_platform'].map(tracking_spec['platform_to_lib_type'])

    data = [
        (sample_name, sample_df) for sample_name, sample_df in df.groupby('sample_name')
    ]

    return data[request.param]


@mark.parametrize(argnames='sample_name_df', argvalues=range(13), indirect=True)
def test_program_from_lib_types(
    sample_name_df: tuple[str, pd.DataFrame], dirs: dict[str, Path]
):
    sample_name, sample_df = sample_name_df
    aggregated = pd.Series(index=['sample_name'], data=sample_name)
    aggregated['libraries'] = tuple(sample_df.index)

    # TODO: the below is literally copy-pasted from the function its testing. fix this.
    library_types = tuple(sample_df['library_types'].sort_values())
    aggregated['tool'], aggregated['command'], aggregated['reference_dirs'] = LIB_TYPES_TO_PROGRAM.get(library_types, (None, None, None))  # type: ignore
    aggregated['reference_dirs'] = [
        dirs['references'] / ref_child_dir
        for ref_child_dir in aggregated['reference_dirs']
    ]

    for col in ('10x_platform', 'fastq_paths', 'library_types'):
        aggregated[col] = tuple(sample_df[col])

    for col in ('is_nuclei', 'project', 'tag_id'):
        aggregated[col] = sample_df[col].drop_duplicates().item()

    aggregated['n_cells'] = sample_df['n_cells'].max()

    output = samplesheet.program_from_lib_types(
        sample_df, ref_parent_dir=dirs['references']
    )

    assert output[aggregated.index].equals(aggregated)


def test_get_latest_version():
    latest_versions = {
        'cellranger': '7.1.0',
        'cellranger-atac': '2.1.0',
        'cellranger-arc': '2.0.0',
        'spaceranger': '2.1.0',
        'citeseq-count': '1.4.5',
    }
    for tool, version in latest_versions.items():
        assert samplesheet.get_latest_version(tool) == version


@mark.parametrize(argnames=('sample_name', 'sanitized'), argvalues=[('goodname-stuff123', 'goodname-stuff123'), ('space name_underscore', 'space-name-underscore'), ("!@#$%^&*'bad_chars and (spaces)", 'bad-chars-and-spaces')])
def test_sanitize_samplename(sample_name, sanitized):
    assert samplesheet.sanitize_samplename(sample_name) == sanitized
