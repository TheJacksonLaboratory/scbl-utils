from pathlib import Path

import pandas as pd
from gspread import Client
from pytest import fixture

from scbl_utils.utils import gdrive
from scbl_utils.utils.defaults import SCOPES


@fixture(scope='session')
def gclient(gdrive_config_files) -> Client:
    return gdrive.login(
        scopes=SCOPES, filename=gdrive_config_files['service-account.json']
    )


def test_trackingsheet_to_df(
    dirs: dict[str, Path], gclient: Client, tracking_spec: dict
):
    # Get trackingsheet as df
    trackingsheet = gdrive.GSheet(
        client=gclient, properties={'id': tracking_spec['id']}
    )
    tracking_df = trackingsheet.to_df(
        col_renaming=tracking_spec['columns'],
    )

    tracking_df['n_cells'] = pd.to_numeric(tracking_df['n_cells'].str.replace(',', ''), errors='coerce')

    # Load the expected trackingsheet as df and compare
    expected_df = pd.read_csv(dirs['data'] / 'test-trackingsheet.csv', index_col=0)
    tracking_df = tracking_df[expected_df.columns]
    assert tracking_df.equals(expected_df)


@fixture
def input_cols():
    return ['libraries', 'sample_name', 'project', 'tool', 'reference_dirs']


@fixture
def output_cols():
    return ['tool_version', 'reference_path']


@fixture
def full_index(input_cols, output_cols):
    return input_cols + output_cols


# TODO: the below tests are too hardcoded. do something better
def test_get_old_project_params(
    gclient: Client,
    metrics_spec: dict,
    dirs: dict[str, Path],
    input_cols: list[str],
    output_cols: list[str],
    full_index: list[str],
):
    vdj_dir = dirs['references'] / '10x-vdj'

    input_output = pd.Series(
        index=full_index,
        data=[
            ('SC9900000',),
            "5' VDJ Sample 0",
            'SCP99-000',
            'cellranger',
            [vdj_dir],
            '6.1.1',
            [str(vdj_dir / 'test-refdata')],
        ],
    )

    result = gdrive.get_project_params(
        input_output[input_cols],
        gclient=gclient,
        metrics_dir_id=metrics_spec['dir_id'],
        col_renaming=metrics_spec['columns'],
    )


    assert result.to_dict() == input_output[output_cols].to_dict()


def test_get_new_project_params(
    gclient: Client,
    metrics_spec: dict,
    dirs: dict[str, Path],
    input_cols: list[str],
    output_cols: list[str],
    full_index: list[str],
):
    atac_dir = dirs['references'] / '10x-atac'

    input_output = pd.Series(
        index=full_index,
        data=[
            ('SC9900016',),
            'ATAC Sample 1',
            'SCP99-002',
            'cellranger-atac',
            [atac_dir],
            '2.1.0',
            [str(atac_dir / 'test-refdata')],
        ],
    )

    result = gdrive.get_project_params(
        input_output[input_cols],
        gclient=gclient,
        metrics_dir_id=metrics_spec['dir_id'],
        col_renaming=metrics_spec['columns'],
    )

    assert result.to_dict() == input_output[output_cols].to_dict()
