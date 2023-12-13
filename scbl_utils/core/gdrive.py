"""
This module contains functions related to Google Drive that are used in
`main.py` to create a command-line interface.

Functions:
"""
from typing import Any, Hashable

import gspread as gs
import pandas as pd


def gsheet_to_compatible_records(
    spread: gs.Spreadsheet, worksheet_id: str, columns: dict[str, str], head: int = 0
) -> list[dict[Hashable, Any]]:
    """
    Convert a Google Sheet to a list of records that are compatible with
    the database
    """
    # TODO: this will require error-checking for cases where the sheet
    # has duplicate column names (idk how that's a thing) and

    sheet = spread.get_worksheet_by_id(worksheet_id)
    raw_records = sheet.get_records(head=head)
    raw_df = pd.DataFrame.from_records(raw_records)
    return raw_df.rename(columns=columns).to_dict(orient='records')
