import sqlite3
from pathlib import Path
from typer.testing import CliRunner

from .db_fixtures import config_dir, delivery_parent_dir, valid_data_dir, db_path
from scbl_utils.main import app


def test_init_db(config_dir: Path, valid_data_dir: Path, db_path: Path):
    """
    Test that init-db works properly.
    """
    runner = CliRunner()

    args = ['--config-dir', str(config_dir), 'init-db', str(valid_data_dir)]
    result = runner.invoke(app, args=args)

    with sqlite3.connect(db_path) as conn:
        # TODO: get all tables and check that they match the expected
        # data. This requires writing the expected 
        conn.row_factory = sqlite3.Row
    
    assert False