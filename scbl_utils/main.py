from collections.abc import Generator
from csv import QUOTE_STRINGS
from csv import reader as csv_reader
from functools import cache, cached_property
from os import environ, stat
from pathlib import Path

import fire
import gspread as gs
from pydantic import DirectoryPath, FilePath, computed_field, validate_call
from pydantic.dataclasses import dataclass
from rich.console import Console
from rich.traceback import install
from scbl_db import ORDERED_MODELS, Base
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session, sessionmaker
from yaml import safe_load

from .config_models.db import DBConfig
from .config_models.gdrive import SpreadsheetConfig
from .config_models.system import SystemConfig
from .data_io import DataToInsert
from .pydantic_model_config import strict_config

console = Console()
install(console=console, max_frames=1)


@dataclass(config=strict_config, frozen=True)
class SCBLUtils:
    """A set of command-line utilities that facilitate data processing at the Single Cell Biology Lab at the Jackson Laboratory."""

    config_dir: DirectoryPath = Path('/sc/service/etc/.config/scbl-utils')

    @computed_field
    @cached_property
    @validate_call(validate_return=True)
    def _db_config_path(self) -> FilePath:
        return self.config_dir / 'db.yml'

    @computed_field
    @cached_property
    @validate_call(validate_return=True)
    def _system_config_path(self) -> FilePath:
        return self.config_dir / 'system.yml'

    @computed_field
    @cached_property
    @validate_call(validate_return=True)
    def _gdrive_config_dir(self) -> DirectoryPath:
        return self.config_dir / 'google-drive'

    @computed_field
    @cached_property
    @validate_call(validate_return=True)
    def _gdrive_credential_path(self) -> FilePath:
        return self._gdrive_config_dir / 'service-account.json'

    @computed_field
    @cached_property
    @validate_call(validate_return=True)
    def _tracking_sheet_spec_dir(self) -> DirectoryPath:
        return self._gdrive_config_dir / 'tracking_sheets'

    @cache
    def _db_sessionmaker(self: 'SCBLUtils') -> sessionmaker[Session]:
        raw_db_config = safe_load(self._db_config_path.read_bytes())
        db_config = DBConfig.model_validate(raw_db_config)

        url = URL.create(**db_config.model_dump())
        engine = create_engine(url)
        Base.metadata.create_all(engine)

        return sessionmaker(engine)

    @computed_field(repr=False)
    @cached_property
    def _system_config(self: 'SCBLUtils') -> SystemConfig:
        raw_system_config = safe_load(self._system_config_path.read_bytes())
        system_config = SystemConfig.model_validate(raw_system_config)

        return system_config

    @cache
    def _gclient(self: 'SCBLUtils') -> gs.Client:
        return gs.service_account(filename=self._gdrive_credential_path)

    @computed_field
    @cached_property
    def _tracking_sheet_specs(
        self: 'SCBLUtils',
    ) -> Generator[SpreadsheetConfig, None, None]:
        tracking_sheet_specs = (
            safe_load(path.read_bytes())
            for path in self._tracking_sheet_spec_dir.iterdir()
        )
        return (SpreadsheetConfig.model_validate(spec) for spec in tracking_sheet_specs)

    @validate_call
    def fill_db(self, data_dir: DirectoryPath | None = None) -> None:
        environ.update(self._system_config.model_dump(mode='json'))
        if data_dir is not None:
            self._directory_to_db(data_dir)

        self._gdrive_to_db()

    def _directory_to_db(self, data_dir: Path):
        session_maker = self._db_sessionmaker()

        for model_name, model in ORDERED_MODELS.items():
            data_path = data_dir / f'{model_name}.csv'

            if not data_path.is_file():
                continue

            if stat(data_path).st_size == 0:
                continue

            with data_path.open() as f:
                data = csv_reader(f, quoting=QUOTE_STRINGS)
                columns = tuple(next(data))

                with session_maker.begin() as session:
                    DataToInsert(
                        columns=columns,
                        data=data,
                        model=model,
                        session=session,
                        source=data_path,
                    ).to_db()

    def _gdrive_to_db(self):
        pass


def main():
    fire.Fire(SCBLUtils)
