from datetime import date

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


class TestGoogleSheetsValueRange:
    def test_to_df(self):
        values = [
            ['foo', 'bar'],  # Extraneous row
            [],  # An empty row
            [
                'name',
                '',
                'date received',
                'column',
            ],  # A header with a missing column name and an extraneous column
            ['sample1', 'foo', '1999-01-01', 'bar'],  # A correct row
            ['null', 'null', 'null'],  # An entirely empty row
            [
                'sample2',
                'foo',
                '1999-01-01',
                'bar',
            ],  # The first row in a pair of merged rows
            ['', 'foo', '1999-01-01', 'bar'],  # The second row in a pair of merged rows
            ['sample3', 'foo', 'null', 'bar'],  # A row with required data missing
        ]

        column_configs = {
            'name': GoogleColumnConfig(targets={'ChromiumSample.name'}),
            'date received': GoogleColumnConfig(
                targets={'ChromiumSample.date_received'}
            ),
        }
        worksheet_config = GoogleWorksheetConfig(
            column_to_targets=column_configs,
            column_to_type={'date received': 'pl.Date'},
            empty_means_drop={'date received'},
            header=2,
            replace={'': None, 'null': None},
            forward_fill_nulls=['name'],
        )

        value_range = GoogleSheetsValueRange(
            values=values, range='Sheet1', majorDimension='ROWS'
        )

        the_date = date(1999, 1, 1)
        expected_data = {
            'name': ['sample1', 'sample2', 'sample2'],
            'date received': [the_date, the_date, the_date],
        }

        assert_frame_equal(
            value_range.to_lf(worksheet_config), pl.DataFrame(expected_data)
        )


class TestGoogleSheetsResponse:
    def test_to_dfs(self):
        library_id_column_config = GoogleColumnConfig(
            targets={'ChromiumDataSet.libraries.id', 'ChromiumLibrary.id'}
        )
        sample_name_column_config = GoogleColumnConfig(
            targets={'ChromiumDataSet.samples.name'}
        )
        library_type_column_config = GoogleColumnConfig(
            targets={'ChromiumDataSet.assay.name'},
            replace={'ChromiumDataSet.assay.name': {'multiome rna': 'multiome'}},
        )

        worksheet_configs = {
            'sheet1': GoogleWorksheetConfig(
                column_to_targets={
                    'library ID': library_id_column_config,
                    'sample name': sample_name_column_config,
                    'library type': library_type_column_config,
                }
            ),
            'sheet2': GoogleWorksheetConfig(
                column_to_targets={
                    'library ID': library_id_column_config,
                    'sample name': sample_name_column_config,
                }
            ),
        }
        spreadsheet_config = GoogleSpreadsheetConfig(
            worksheet_configs=worksheet_configs,
            merge_strategies={
                'ChromiumLibrary': MergeStrategy(
                    merge_on='ChromiumLibrary.id', order=['sheet2', 'sheet1']
                ),
                'ChromiumDataSet': MergeStrategy(
                    merge_on='ChromiumDataSet.libraries.id', order=['sheet2', 'sheet1']
                ),
            },
        )

        sheet1 = GoogleSheetsValueRange(
            range='sheet1',
            values=[
                ['library ID', 'sample name', 'library type'],
                ['lib0', 'sample0', 'multiome rna'],
                ['lib1', 'sample-pool0', 'flex'],
            ],
            majorDimension='ROWS',
        )

        sheet2 = GoogleSheetsValueRange(
            range='sheet2',
            values=[
                ['library ID', 'sample name'],
                ['lib1', 'sample1'],
                ['lib1', 'sample2'],
            ],
            majorDimension='ROWS',
        )

        response = GoogleSheetsResponse(
            spreadsheetId='spreadsheet', valueRanges=[sheet1, sheet2]
        )
        result_dfs = response.to_dfs(spreadsheet_config)

        expected_dfs = {
            'ChromiumLibrary': pl.DataFrame(
                {'ChromiumLibrary.id': ['lib0', 'lib1', 'lib1']}
            ),
            'ChromiumDataSet': pl.DataFrame(
                {
                    'ChromiumDataSet.libraries.id': ['lib0', 'lib1', 'lib1'],
                    'ChromiumDataSet.samples.name': ['sample0', 'sample1', 'sample2'],
                    'ChromiumDataSet.assay.name': ['multiome', 'flex', 'flex'],
                }
            ),
        }

        assert expected_dfs.keys() == result_dfs.keys()

        for key in expected_dfs:
            assert_frame_equal(expected_dfs[key], result_dfs[key])
