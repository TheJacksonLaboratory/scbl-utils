from pathlib import Path

from pytest import fixture
from scbl_db import Institution
from sqlalchemy import select

from scbl_utils.main import SCBLUtils


class TestSCBLUtils:
    @fixture
    def cli(self, config_dir: Path) -> SCBLUtils:
        return SCBLUtils(config_dir=config_dir)

    def test_db_session(self, cli: SCBLUtils):
        with cli._db_sessionmaker.begin() as session:
            institution = Institution(
                ror_id='02kzs4y22', email_format='{first_name}.{last_name}@jax.org'
            )
            session.add(institution)

        with cli._db_sessionmaker.begin() as session:
            institutions_in_db = session.execute(select(Institution)).scalars().all()

            assert len(institutions_in_db) == 1

    def test_directory_to_db_correct_n_rows(self, cli: SCBLUtils):
        data_dir = Path(__file__).parent / 'data'
        cli._directory_to_db(data_dir)

        with cli._db_sessionmaker.begin() as session:
            institutions_in_db = session.execute(select(Institution)).scalars().all()
            assert len(institutions_in_db) == 1

    def test_gdrive_to_db_correct_n_rows(self, cli: SCBLUtils):
        cli._gdrive_to_db()

        with cli._db_sessionmaker.begin() as session:
            institutions_in_db = session.execute(select(Institution)).scalars().all()
            assert len(institutions_in_db) == 1

    def test_fill_db_correct_n_rows(self, cli: SCBLUtils):
        data_dir = Path(__file__).parent / 'data'
        cli.fill_db(data_dir)

        with cli._db_sessionmaker.begin() as session:
            institutions_in_db = session.execute(select(Institution)).scalars().all()
            assert len(institutions_in_db) == 2
