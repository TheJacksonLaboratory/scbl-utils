from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from typer.testing import CliRunner

from scbl_utils.db_models.data import Lab
from scbl_utils.main import app

from .db_fixtures import (
    config_dir,
    db_path,
    db_session,
    delivery_parent_dir,
    valid_data,
)


def test_init_db(
    config_dir: Path, valid_data: tuple[Path, dict], db_session: sessionmaker[Session]
):
    """
    Test that init-db correctly assigns institutions and people to labs.
    """
    runner = CliRunner()

    args = ['--config-dir', str(config_dir), 'init-db', str(valid_data[0])]
    _ = runner.invoke(app, args=args)

    lab_relationships = valid_data[1]
    with db_session.begin() as session:
        stmt = select(Lab)
        labs = session.execute(stmt).scalars().all()
        
        assert all(
            lab.institution_id == lab_relationships[lab.id]['institution_id']
            for lab in labs
        )
        assert all(lab.pi_id == lab_relationships[lab.id]['pi_id'] for lab in labs)
