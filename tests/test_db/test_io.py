import sqlite3
from pathlib import Path
from typer.testing import CliRunner

from .db_fixtures import config_dir, delivery_parent_dir, valid_data_dir, db_path
from scbl_utils.main import app


def test_init_db(config_dir: Path, valid_data_dir: Path, db_path: Path):
    """
    Test that init-db works properly. Because the model testing ensures
    that the models process data corre, this 
    test ensures that rows are correctly linked
    """
    runner = CliRunner()

    args = ['--config-dir', str(config_dir), 'init-db', str(valid_data_dir)]
    result = runner.invoke(app, args=args)

    with sqlite3.connect(db_path) as conn:
        # TODO: get all tables and check that they match the expected
        # data.
        conn.row_factory = sqlite3.Row
    