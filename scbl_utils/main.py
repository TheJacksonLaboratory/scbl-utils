import logging
from collections.abc import Generator
from functools import cached_property
from os import environ
from pathlib import Path

import fire
import polars as pl
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
from .data_io import DataInserter
from .gdrive import GoogleSheetsResponse
from .pydantic_model_config import strict_config

console = Console()
install(console=console)


@dataclass(config=strict_config, frozen=True)
class SCBLUtils:
    """A set of command-line utilities that facilitate data processing at the Single Cell Biology Lab at the Jackson Laboratory."""

    config_dir: DirectoryPath = Path('/sc/service/etc/.config/scbl-utils')
    log_dir: Path = Path.cwd() / 'scbl-utils_log'

    def __post_init__(self) -> None:
        self.log_dir.mkdir(exist_ok=True, parents=True)

        root_logger = logging.getLogger(__package__)

        for model_name in ORDERED_MODELS:
            handler = logging.FileHandler(self.log_dir / f'{model_name}.log', mode='w')
            logger = logging.getLogger(f'{root_logger.name}.{model_name}')

            logger.addHandler(handler)

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

        return sessionmaker(engine, autoflush=False)

    @computed_field
    @cached_property
    def _system_config(self: 'SCBLUtils') -> SystemConfig:
        raw_system_config = safe_load(self._system_config_path.read_bytes())
        system_config = SystemConfig.model_validate(raw_system_config)

        return system_config

    @computed_field
    @cached_property
    def _google_sheets_resource(self: 'SCBLUtils') -> Resource:
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

            with self._db_sessionmaker.begin() as session:
                data = pl.read_csv(data_path, null_values='')

                DataInserter(
                    data=data, session=session, model=model, source=data_path
                ).to_db()

    def _gdrive_to_db(self):
        for config in self._tracking_sheet_configs:
            # TODO: make this more robust by keeping a record of what we have ingested from google drive in the database
            google_sheet_response = (
                self._google_sheets_resource.spreadsheets()
                .values()
                .batchGet(
                    spreadsheetId=config.spreadsheet_id,
                    ranges=list(config.worksheet_configs.keys()),
                    majorDimension='ROWS',
                )
                .execute()
            )
            google_sheet_response = GoogleSheetsResponse.model_validate(
                google_sheet_response
            )

            spreadsheet_as_dfs = google_sheet_response.to_dfs(config)

            for model_name, model in ORDERED_MODELS.items():
                if model_name not in spreadsheet_as_dfs:
                    continue

                with self._db_sessionmaker.begin() as session:
                    data_inserter = DataInserter(
                        data=spreadsheet_as_dfs[model_name],
                        model=model,
                        session=session,
                        source=config.spreadsheet_id,
                    )

                    data_inserter.to_db()


def main():
    fire.Fire(SCBLUtils)
