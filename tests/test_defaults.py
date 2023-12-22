from scbl_utils.db_models.bases import Base
from scbl_utils.defaults import (
    DATA_SCHEMAS,
    DB_INIT_FILES,
    SPLIT_TABLES_JOIN_ON_COLUMNS,
)


def test_something_to_rename():
    """
    Test that the keys of `CSV_SCHEMAS` and `DB_INIT_FILES` are the same
    and that they are a subset of `csv_to_model.keys()` (in main.py).
    """
    assert DATA_SCHEMAS.keys() == {path.name for path in DB_INIT_FILES}

    tables = {f'{model.__tablename__}.csv' for model in Base.__subclasses__()}
    assert DATA_SCHEMAS.keys() <= tables
    assert SPLIT_TABLES_JOIN_ON_COLUMNS.keys() <= tables
