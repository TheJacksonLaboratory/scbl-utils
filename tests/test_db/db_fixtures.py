from pathlib import Path
from pytest import fixture
from sqlalchemy.orm import Session
from scbl_utils.db_models.bases import Base
from scbl_utils.core import new_db_session
@fixture
def tmp_db_session(tmp_path: Path):
    """
    Create a temporary database for testing.
    """
    db_path = tmp_path / 'test.db'
    Session = new_db_session(Base, drivername='sqlite', database=str(db_path.absolute()))
    return Session