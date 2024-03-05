from csv import DictReader
from pathlib import Path

from pytest import fixture
from scbl_db import ORDERED_MODELS
from sqlalchemy import select

from scbl_utils.main import SCBLUtils


class TestSCBLUtils:
    @fixture
    def cli(self, config_dir: Path) -> SCBLUtils:
        return SCBLUtils(config_dir=config_dir)

    @fixture
    def data_dir(self) -> Path:
        return Path(__file__).parent / 'data'

    def test_correct_n_rows(self, cli: SCBLUtils, data_dir: Path) -> None:
        cli._directory_to_db(data_dir)

        for file in data_dir.iterdir():
            model = ORDERED_MODELS[file.stem]

            with file.open() as f, cli._db_sessionmaker.begin() as s:
                data = tuple(DictReader(f))
                model_instances_in_db = s.execute(select(model)).scalars().all()

                assert len(model_instances_in_db) == len(data)
