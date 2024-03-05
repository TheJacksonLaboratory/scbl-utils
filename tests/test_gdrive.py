import gspread as gs

from scbl_utils.config import SpreadsheetConfig, TargetConfig, WorksheetConfig
from scbl_utils.gdrive import GSpreadsheet


def test_something():
    column_config = TargetConfig(
        targets={
            'Institution.name',
            'Person.first_name',
            'Person.last_name',
            'Lab.name',
        },
        replace={},
    )
    sheet_config = WorksheetConfig(
        replace={},
        columns_to_targets={'institution': column_config},
        type_converters={},
        empty_means_drop=set(),
    )
    configspreadsheet = SpreadsheetConfig(
        spreadsheet_url='https://docs.google.com/spreadsheets/d/1oMgSTEUBIgO4XlhL4jPlsH4dvAGjfqvQMLCVp3-YdPA/edit#gid=0',
        worksheet_configs={'0': sheet_config},
        worksheet_priority=['0'],
    )

    gclient = gs.service_account(
        '/Users/saida/.config/scbl-utils/google-drive/service-account.json'
    )
    df = GSpreadsheet(config=configspreadsheet, gclient=gclient)
    for sheet in df.worksheets:
        print(*sheet.as_records['Institution'][:5], sep='\n')
