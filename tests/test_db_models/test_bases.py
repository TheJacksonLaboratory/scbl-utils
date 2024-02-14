from string import punctuation, whitespace

from pytest import raises

from scbl_utils.db_models.base import *
from scbl_utils.db_models.column_types import SamplesheetString, StrippedString
from scbl_utils.db_models.data_models.chromium import *

from ..fixtures.db.data import delivery_parent_dir
from ..fixtures.db.utils import complete_db_objects


class TestBaseModel:
    """
    Tests for the base model.
    """

    def test_get_model(self, complete_db_objects: dict[str, Base]):
        """
        Test that the `get_model` method returns the correct model.
        """
        for tablename, obj in complete_db_objects.items():
            assert Base.get_model(tablename) == type(obj)

    def test_get_model_raises_keyerror(self):
        """
        Test that the `get_model` method raises a `KeyError` when
        passed an invalid table name.
        """
        with raises(KeyError):
            Base.get_model('invalid_table_name')


class TestStrippedString:
    """
    Tests for the `StrippedString` type.
    """

    def test_process_bind_param(self):
        """
        Test that the `process_bind_param` method strips whitespace from
        the ends of the string but does not modify the string otherwise.
        """
        string = f'{whitespace}{punctuation}string{punctuation}{whitespace}'

        assert (
            StrippedString().process_bind_param(string=string, dialect='')
            == f'{punctuation}string{punctuation}'
        )


class TestSamplesheetString:
    """
    Tests for the `SamplesheetString` type.
    """

    def test_process_bind_param(self):
        """
        Test that the `process_bind_param` method removes illegal
        characters from the string.
        """
        illegal_punctuation = sub(pattern='[-,_]', repl='', string=punctuation)
        string = f'{illegal_punctuation}string{whitespace}string{illegal_punctuation}'

        assert (
            SamplesheetString().process_bind_param(string=string, dialect='')
            == 'string-string'
        )
