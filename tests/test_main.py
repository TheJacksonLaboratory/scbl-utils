from pathlib import Path

import pandas as pd
from pytest import MonkeyPatch
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from typer.testing import CliRunner

from scbl_utils.db_models import bases, data, definitions
from scbl_utils.main import app

from .fixtures.db_fixtures import (
    config_dir,
    db_path,
    delivery_parent_dir,
    n_rows_per_table,
    table_relationships,
    test_db_session,
    valid_data_dir,
)


def test_init_db(
    config_dir: Path,
    monkeypatch: MonkeyPatch,
    n_rows_per_table: dict[str, int],
    test_db_session: sessionmaker[Session],
    valid_data_dir: Path,
    table_relationships: dict[tuple[str, str], pd.DataFrame],
):
    """
    Test that init-db correctly assigns the foreign keys of linked
    tables.
    """
    # Run the command
    runner = CliRunner()

    args = ['--config-dir', str(config_dir), 'init-db', str(valid_data_dir)]
    monkeypatch.setattr('rich.prompt.Prompt.ask', lambda *args, **kwargs: None)

    result = runner.invoke(app, args=args, input='\n', color=True)
    print(result.stdout)
    assert result.exit_code == 0

    with test_db_session.begin() as session:
        for tablename, n_rows in n_rows_per_table.items():
            model = bases.Base.get_model(tablename)
            stmt = select(model)
            rows = session.execute(stmt).scalars().all()
            assert len(rows) == n_rows

        # Iterate over each pair of tables that should be connected
        for (
            left_table_name,
            right_table_name,
        ), joined_df in table_relationships.items():
            left_table = bases.Base.get_model(left_table_name)

            stmt = select(left_table)
            left_objects = session.execute(stmt).scalars().all()

            # The order of objects returned by the database should match
            # the order of the rows in the joined DataFrame, so just
            # check in order.
            for i, obj in enumerate(left_objects):
                assert (
                    getattr(obj, f'{right_table_name}_id')
                    == joined_df.loc[i, f'{right_table_name}_id']
                )
