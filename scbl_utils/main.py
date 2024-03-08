from collections.abc import Generator
from csv import QUOTE_STRINGS
from csv import reader as csv_reader
from functools import cache, cached_property
from os import environ, stat
from pathlib import Path

import fire
from googleapiclient.discovery import Resource, build
from oauth2client.service_account import ServiceAccountCredentials
from pydantic import DirectoryPath, FilePath, computed_field, validate_call
from pydantic.dataclasses import dataclass
from rich.console import Console
from rich.traceback import install
from scbl_db import ORDERED_MODELS, Base
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session, sessionmaker
from yaml import safe_load

from .config import DBConfig, GoogleSpreadsheetConfig, SystemConfig
from .data_io import DataToInsert
from .gdrive import GoogleSheetResponse
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
    def _tracking_sheet_config_dir(self) -> DirectoryPath:
        return self._gdrive_config_dir / 'tracking_sheets'

    @computed_field
    @cached_property
    def _db_sessionmaker(self: 'SCBLUtils') -> sessionmaker[Session]:
        raw_db_config = safe_load(self._db_config_path.read_bytes())
        db_config = DBConfig.model_validate(raw_db_config)

        url = URL.create(**db_config.model_dump())
        engine = create_engine(url)
        Base.metadata.create_all(engine)

        return sessionmaker(engine)

    @computed_field
    @cached_property
    def _system_config(self: 'SCBLUtils') -> SystemConfig:
        raw_system_config = safe_load(self._system_config_path.read_bytes())
        system_config = SystemConfig.model_validate(raw_system_config)

        return system_config

    @computed_field
    @cached_property
    def _google_resource(self: 'SCBLUtils') -> Resource:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self._gdrive_credential_path
        )
        return build(serviceName='sheets', version='v4', credentials=credentials)

    @computed_field
    @cached_property
    def _tracking_sheet_configs(
        self: 'SCBLUtils',
    ) -> Generator[GoogleSpreadsheetConfig, None, None]:
        tracking_sheet_configs = (
            safe_load(path.read_bytes())
            for path in self._tracking_sheet_config_dir.iterdir()
        )
        return (
            GoogleSpreadsheetConfig.model_validate(config)
            for config in tracking_sheet_configs
        )

    @validate_call
    def fill_db(self, data_dir: DirectoryPath | None = None) -> None:
        environ.update(self._system_config.model_dump(mode='json'))
        if data_dir is not None:
            self._directory_to_db(data_dir)

        self._gdrive_to_db()

    def _directory_to_db(self, data_dir: Path):
        for model_name, model in ORDERED_MODELS.items():
            data_path = data_dir / f'{model_name}.csv'

            if not data_path.is_file():
                continue

            if stat(data_path).st_size == 0:
                continue

            with data_path.open() as f, self._db_sessionmaker.begin() as session:
                data = csv_reader(f, quoting=QUOTE_STRINGS)
                columns = tuple(next(data))

                DataToInsert(
                    columns=columns,
                    data=data,
                    model=model,
                    session=session,
                    source=data_path,
                ).to_db()

    def _gdrive_to_db(self):
        for config in self._tracking_sheet_configs:
            # TODO: make this more robust by keeping a record of what we have ingested from google drive in the database
            google_sheet_response = (
                self._google_resource.spreadsheets()
                .values()
                .batchGet(
                    spreadsheetId=config.spreadsheet_id,
                    ranges=config.worksheet_configs.keys(),
                    majorDimension='COLUMNS',
                )
                .execute()
            )
            google_sheet_response = GoogleSheetResponse.model_validate(
                google_sheet_response
            )

            google_sheet_response.to_lfs(config)


def main():
    fire.Fire(SCBLUtils)
