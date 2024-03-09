import polars as pl
from polars.testing import assert_frame_equal
from pytest import fixture

from scbl_utils.config import (
    GoogleColumnConfig,
    GoogleSpreadsheetConfig,
    GoogleWorksheetConfig,
    MergeStrategy,
)
from scbl_utils.gdrive import GoogleSheetsResponse, GoogleSheetsValueRange

# TODO: rewrite these tests a bit


@fixture
def google_spreadsheet_config():
    main_sheet_config = GoogleWorksheetConfig(
        column_to_targets={
            'library id': GoogleColumnConfig(targets={'ChromiumLibrary.id'}),
            'sample name': GoogleColumnConfig(
                targets={
                    'ChromiumLibrary.data_set.name',
                    'ChromiumLibrary.data_set.samples.name',
                }
            ),
        }
    )

    multiplexing_sheet_config = GoogleWorksheetConfig(
        column_to_targets={
            'library id': GoogleColumnConfig(targets={'ChromiumLibrary.id'}),
            'dataset name': GoogleColumnConfig(
                targets={'ChromiumLibrary.data_set.name'}
            ),
            'sample name': GoogleColumnConfig(
                targets={'ChromiumLibrary.data_set.samples.name'}
            ),
        }
    )

    return GoogleSpreadsheetConfig(
        spreadsheet_id='id',
        worksheet_configs={
            'main_sheet': main_sheet_config,
            'multiplexing_sheet': multiplexing_sheet_config,
        },
        merge_strategies={
            'ChromiumLibrary': MergeStrategy(
                on='ChromiumLibrary.id', order=['multiplexing_sheet', 'main_sheet']
            )
        },
    )


class TestGoogleSheetResponse:
    @fixture
    def main_sheet(self) -> GoogleSheetsValueRange:
        range = 'main_sheet'
        major_dimension = 'ROWS'
        values = [
            ['library id', 'sample name', 'extraneous column'],
            ['SC0', 'SC1', 'SC2', 'SC3'],
            ['sample0', 'sample1', 'sample2', 'sample3'],
            ['foo', 'bar', 'baz', 'bah'],
        ]

        return GoogleSheetsValueRange(
            range=range, majorDimension=major_dimension, values=values
        )

    @fixture
    def multiplexing_sheet(self) -> GoogleSheetsValueRange:
        range = 'multiplexing_sheet'
        major_dimension = 'ROWS'
        values = [
            ['library id', 'dataset name', 'sample name'],
            ['SC1', 'SC1'],
            ['multiplexed_dataset1', 'multiplexed_dataset1'],
            ['multiplexed_sample1', 'multiplexed_sample2'],
        ]

        return GoogleSheetsValueRange(
            range=range, majorDimension=major_dimension, values=values
        )

    @fixture
    def google_sheet_response(
        self,
        main_sheet: GoogleSheetsValueRange,
        multiplexing_sheet: GoogleSheetsValueRange,
    ):
        return GoogleSheetsResponse(
            spreadsheetId='spreadsheet_id',
            valueRanges=[main_sheet, multiplexing_sheet],
        )

    # TODO: see if we can make this a bit more sophisticated and less hardcoded
    def test_sheet_splitting(
        self,
        google_sheet_response: GoogleSheetsResponse,
        google_spreadsheet_config: GoogleSpreadsheetConfig,
    ):
        result_dfs = google_sheet_response.to_dfs(google_spreadsheet_config)

        expected_dfs = {}

        expected_dfs['ChromiumLibrary'] = {
            'ChromiumLibrary.id': ['SC0', 'SC1', 'SC1', 'SC2', 'SC3'],
            'ChromiumLibrary.data_set.name': [
                'sample0',
                'multiplexed_dataset1',
                'multiplexed_dataset1',
                'sample2',
                'sample3',
            ],
            'ChromiumLibrary.data_set.samples.name': [
                'sample0',
                'multiplexed_sample1',
                'multiplexed_sample2',
                'sample2',
                'sample3',
            ],
        }

        for key, expected_data in expected_dfs.items():
            assert_frame_equal(result_dfs[key], pl.DataFrame(expected_data))
