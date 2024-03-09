import polars as pl
from polars.testing import assert_frame_equal
from pytest import fixture

from scbl_utils.config import (
    GoogleColumnConfig,
    GoogleSpreadsheetConfig,
    GoogleWorksheetConfig,
)
from scbl_utils.gdrive import GoogleSheetsResponse, GoogleSheetsValueRange


@fixture
def google_spreadsheet_config():
    column_config1 = GoogleColumnConfig(
        targets={'Institution.name', 'Person.first_name'}
    )
    column_config2 = GoogleColumnConfig(
        targets={'Institution.name', 'Person.first_name'}
    )

    worksheet_config1 = GoogleWorksheetConfig(
        column_to_targets={'column1': column_config1}, replace={'': None}
    )
    worksheet_config2 = GoogleWorksheetConfig(
        column_to_targets={'column2': column_config2}, replace={'': None}
    )

    return GoogleSpreadsheetConfig(
        spreadsheet_id='id',
        worksheet_configs={
            'worksheet1': worksheet_config1,
            'worksheet2': worksheet_config2,
        },
    )


class TestGoogleSheetResponse:
    @fixture
    def google_sheet_value_range1(self) -> GoogleSheetsValueRange:
        range = 'worksheet1'
        major_dimension = 'ROWS'
        values = [['column1', 'other_column'], ['name', 'value'], ['', '']]

        return GoogleSheetsValueRange(
            range=range, majorDimension=major_dimension, values=values
        )

    @fixture
    def google_sheet_value_range2(self) -> GoogleSheetsValueRange:
        range = 'worksheet2'
        major_dimension = 'ROWS'
        values = [['column2'], ['other_name']]

        return GoogleSheetsValueRange(
            range=range, majorDimension=major_dimension, values=values
        )

    @fixture
    def google_sheet_response(
        self,
        google_sheet_value_range1: GoogleSheetsValueRange,
        google_sheet_value_range2: GoogleSheetsValueRange,
    ):
        return GoogleSheetsResponse(
            spreadsheetId='spreadsheet_id',
            valueRanges=[google_sheet_value_range1, google_sheet_value_range2],
        )

    # TODO: see if we can make this a bit more sophisticated and less hardcoded
    def test_sheet_splitting(
        self,
        google_sheet_response: GoogleSheetsResponse,
        google_spreadsheet_config: GoogleSpreadsheetConfig,
    ):
        result_lfs = google_sheet_response.to_lfs(google_spreadsheet_config)
        expected_lfs = {}

        expected_lfs[('Institution', 'worksheet1')] = {'Institution.name': ['name']}
        expected_lfs[('Institution', 'worksheet2')] = {
            'Institution.name': ['other_name']
        }
        expected_lfs[('Person', 'worksheet1')] = {'Person.first_name': ['name']}
        expected_lfs[('Person', 'worksheet2')] = {'Person.first_name': ['other_name']}

        for key, expected_data in expected_lfs.items():
            assert_frame_equal(result_lfs[key], pl.LazyFrame(expected_data))


# def test_something():
#     column_config = GoogleColumnConfig(
#         targets={
#             'Institution.name',
#             'Person.first_name',
#             'Person.last_name',
#             'Lab.name',
#         },
#         replace={},
#     )
#     sheet_config = GoogleWorksheetConfig(
#         replace={},
#         columns_to_targets={'institution': column_config},
#         type_converters={},
#         empty_means_drop=set(),
#     )
#     configspreadsheet = GoogleSpreadsheetConfig(
#         spreadsheet_url='https://docs.google.com/spreadsheets/d/1oMgSTEUBIgO4XlhL4jPlsH4dvAGjfqvQMLCVp3-YdPA/edit#gid=0',
#         worksheet_configs={'0': sheet_config},
#         worksheet_priority=['0'],
#     )

#     gclient = gs.service_account(
#         '/Users/saida/.config/scbl-utils/google-drive/service-account.json'
#     )
#     df = GSpreadsheet(config=configspreadsheet, gclient=gclient)
#     for sheet in df.worksheets:
#         print(sheet.as_insertable_data)
#         print(sheet.as_insertable_data['Person']['columns'])
#         print(sheet.as_insertable_data['Institution']['columns'])
#     assert False
