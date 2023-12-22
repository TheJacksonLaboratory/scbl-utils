from os import getenv
from pathlib import Path

import gspread as gs
from pytest import exit as test_exit
from pytest import fixture


@fixture
def gclient() -> gs.Client:
    service_account_credential_path = getenv('GOOGLE-DRIVE-CREDENTIALS')
    if service_account_credential_path is None:
        test_exit(
            'GOOGLE-DRIVE-CREDENTIALS environment variable not set. Set to the path of the service account credentials JSON file.',
            returncode=1,
        )
    return gs.service_account(Path(service_account_credential_path))


@fixture
def empty_spreadsheet(gclient: gs.Client) -> gs.Spreadsheet:
    return gclient.open_by_url(
        'https://docs.google.com/spreadsheets/d/1aHTLCJomrcQaUplbUx7fKEtu6Zzny51Q074i6WwOvk4'
    )
