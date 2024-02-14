from pathlib import Path

from pytest import fixture
from sqlalchemy.orm import Session, sessionmaker

from scbl_utils.db.core import db_session
from scbl_utils.db.orm.base import Base


@fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """
    Create a temporary database path for testing.
    """
    return tmp_path / 'test.db'


@fixture
def tmp_db_session(tmp_db_path: Path) -> sessionmaker[Session]:
    """
    Create a database session for testing.
    """
    Session = db_session(Base, drivername='sqlite', database=str(tmp_db_path))
    return Session
