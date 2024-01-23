from scbl_utils.db_models.bases import Base
from scbl_utils.db_models.data import *
from scbl_utils.db_models.definitions import *
from scbl_utils.defaults import DATA_INSERTION_ORDER, DATA_SCHEMAS, DB_INIT_FILES


def test_subsets_of_db_tables():
    """
    Test that any collections of database tables are actually subsets of
    all the tables in the database.
    """
    all_tables = {model.__tablename__ for model in Base.__subclasses__()}
    db_init_tables = {path.name for path in DB_INIT_FILES}

    assert DATA_SCHEMAS.keys() == db_init_tables
    assert db_init_tables <= all_tables
    assert all_tables.issuperset(DATA_INSERTION_ORDER)
