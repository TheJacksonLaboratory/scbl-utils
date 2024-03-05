from csv import reader
from os import stat
from pathlib import Path

from pytest import fixture, mark
from scbl_db import ORDERED_MODELS, Base
from sqlalchemy import select

from scbl_utils.main import SCBLUtils


class TestSCBLUtils:
    @fixture
    def cli(self, config_dir: Path) -> SCBLUtils:
        return SCBLUtils(config_dir=config_dir)

    @fixture
    def data_dir(self) -> Path:
        return Path(__file__).parent / 'data'

    @fixture
    def insert_data(self, cli: SCBLUtils, data_dir: Path) -> None:
        cli._directory_to_db(data_dir=data_dir)

    @mark.parametrize(
        argnames=['model_name', 'model'],
        argvalues=[(model_name, model) for model_name, model in ORDERED_MODELS.items()],
    )
    def test_correct_n_rows(
        self,
        cli: SCBLUtils,
        data_dir: Path,
        insert_data: None,
        model_name: str,
        model: type[Base],
    ) -> None:
        data_file = data_dir / f'{model_name}.csv'

        if data_file not in data_dir.iterdir():
            return

        if stat(data_file).st_size == 0:
            return

        with data_file.open() as f:
            csv = reader(f)

            _ = next(csv)
            data = tuple(csv)

            with cli._db_sessionmaker().begin() as s:
                results = s.execute(select(model)).scalars().all()
                assert len(results) == len(data)
