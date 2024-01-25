# TODO: write a test that creates a Google Sheet, fills it with data,
# and then instantiates a core.gdrive.Sheet object from it,
# testing whether the columns are appropriately renamed
from gspread import Spreadsheet

from scbl_utils.core.gdrive import TrackingSheet
from scbl_utils.db_models.bases import Base


class TestTrackingSheet:
    """
    Tests for the gdrive.Sheet class
    """

    pass
