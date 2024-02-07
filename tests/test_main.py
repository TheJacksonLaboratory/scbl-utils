from pathlib import Path
from re import L

import pandas as pd
from pytest import fixture
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from typer.testing import CliRunner

from scbl_utils.db_models.base import Base
from scbl_utils.db_models.data_models.chromium import *
from scbl_utils.db_models.definitions import *
from scbl_utils.defaults import DB_INIT_FILES
from scbl_utils.old_main import app

from .fixtures.db_fixtures import (
    config_dir,
    data_dir,
    db_data,
    db_path,
    delivery_parent_dir,
    other_parent_names,
    table_relationships,
    test_db_session,
)


class TestDatabaseInitialization:
    @fixture
    def run_init_db(self, config_dir: Path, data_dir: Path, test_db_session: Path):
        runner = CliRunner()

        args = ['--config-dir', str(config_dir), 'init-db', str(data_dir)]
        result = runner.invoke(app, args=args, color=True)

        assert result.exit_code == 0, result.stdout

    def test_init_db_adds_correct_n_rows(
        self,
        test_db_session: sessionmaker[Session],
        db_data: dict[str, pd.DataFrame],
        run_init_db: None,
    ):
        with test_db_session.begin() as session:
            for init_file in DB_INIT_FILES:
                tablename = init_file.stem
                model = Base.get_model(tablename)
                stmt = select(model)
                rows = session.execute(stmt).scalars().all()

                expected_n_rows = db_data[tablename].shape[0]
                found_n_rows = len(rows)

                assert found_n_rows == expected_n_rows

    def test_init_db_relationships(
        self,
        test_db_session: sessionmaker[Session],
        table_relationships: dict[tuple[str, str], pd.DataFrame],
        run_init_db: None,
        other_parent_names: dict[str, str],
    ):
        with test_db_session.begin() as session:
            for (
                child_tablename,
                parent_tablename,
            ), joined_df in table_relationships.items():
                child_model = Base.get_model(child_tablename)
                stmt = select(child_model)
                children = session.execute(stmt).scalars().all()
                parent_id_col = (
                    other_parent_names.get(
                        f'{child_tablename}.{parent_tablename}', parent_tablename
                    )
                    + '.id'
                )

                for child in children:
                    assigned_parent = getattr(
                        child, parent_tablename
                    )  # TODO: add a type-hint here?
                    correct_parent_id = joined_df.loc[
                        joined_df[f'{child_tablename}.id'] == child.id, parent_id_col
                    ]

                    if not isinstance(correct_parent_id, pd.Series):
                        pass
                    elif len(correct_parent_id) == 1:
                        correct_parent_id = correct_parent_id.values[0]
                    else:
                        raise ValueError(
                            f'Duplicate children found for {child_tablename} with id {child.id}'
                        )

                    assert assigned_parent.id == correct_parent_id


class TestDatabaseUpdate:
    ...
