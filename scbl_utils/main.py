import logging
from os import environ
from pathlib import Path
from sys import exit as sys_exit

import fire
import gspread as gs
from jsonschema import validate as validate_data
from numpy import nan
from pandas import read_csv
from pydantic import DirectoryPath, Field, model_validator
from pydantic.dataclasses import dataclass
from rich.logging import RichHandler
from sqlalchemy import select
from yaml import safe_load

from .db.core import data_rows_to_db, db_session
from .db.orm.base import Base
from .db.orm.models.data import Platform
from .db.orm.models.platforms.chromium import *
from .db.orm.models.platforms.xenium import *
from .gdrive.core import TrackingSheet
from .json_schemas.config_schemas import (
    DB_SPEC_SCHEMA,
    GDRIVE_PLATFORM_SPEC_SCHEMA,
    SYSTEM_CONFIG_SCHEMA,
)
from .json_schemas.data_schemas import DATA_SCHEMAS
from .validation import validate_directory

logging.basicConfig(
    datefmt='[%X]',
    format='%(message)s',
    handlers=[
        RichHandler(
            rich_tracebacks=True,
            tracebacks_suppress=[fire],
            tracebacks_show_locals=True,
            markup=True,
        )
    ],
)
log = logging.getLogger('rich')


@dataclass(kw_only=True)
class SCBLUtils(object):
    """A set of command-line utilities that facilitate data processing at the Single Cell Biology Lab at the Jackson Laboratory."""

    config_dir: str | Path = Field(
        default=Path('/sc/service/etc/.config/scbl-utils'), validate_default=True
    )

    @model_validator(mode='after')
    def _load_config(self) -> 'SCBLUtils':
        self._data_insertion_order = [
            'Institution',
            'Person',
            'Lab',
            'Platform',
            'Project',
            'LibraryType',
            'Tag',
            'SequencingRun',
            'ChromiumDataSet',
            'Library',
            'ChromiumSample',
            'XeniumRun',
            'XeniumDataSet',
            'XeniumSample',
        ]

        self.config_dir = Path(self.config_dir)

        required_files = {
            'db_spec': 'db/db_spec.yml',
            'gdrive_credentials': 'google-drive/service-account.json',
            'system_config': 'system/config.yml',
        }
        required_directories = {
            'platform_tracking-sheet_specs': 'google-drive/platform_tracking-sheet_specs'
        }

        try:
            validate_directory(
                self.config_dir,
                required_files=required_files.values(),
                required_directories=required_directories.values(),
            )
        except:
            log.exception(
                f'{self.config_dir} does not exist or is missing required files or directories'
            )
            sys_exit(1)

        self._platform_tracking_sheet_specs_dir = (
            self.config_dir / required_directories['platform_tracking-sheet_specs']
        )

        db_spec_path = self.config_dir / required_files['db_spec']
        try:
            self._db_spec: dict[str, str] = safe_load(db_spec_path.read_text())
            validate_data(self._db_spec, schema=DB_SPEC_SCHEMA)
            self._Session = db_session(Base, **self._db_spec)
        except:
            log.exception(f'Error reading or validating {db_spec_path}')
            sys_exit(1)

        gdrive_credential_path = self.config_dir / required_files['gdrive_credentials']
        try:
            self._gclient = gs.service_account(filename=gdrive_credential_path)
        except:
            log.exception('Error logging into Google Drive.')
            sys_exit(1)

        system_config_path = self.config_dir / required_files['system_config']
        try:
            self._system_config: dict[str, str] = safe_load(
                system_config_path.read_text()
            )
            validate_data(self._system_config, schema=SYSTEM_CONFIG_SCHEMA)
            environ.update(self._system_config)
        except:
            log.exception(f'Error reading or validating {system_config_path}')
            sys_exit(1)

        return self

    def init_db(self, db_init_data_dir: str, sync_with_gdrive: bool = True):
        data_dir = Path(db_init_data_dir)

        added_data_sources = []
        filenames = [f'{model_name}.csv' for model_name in self._data_insertion_order]
        with self._Session.begin() as session:
            for filename in filenames:
                data_path = data_dir / filename

                if not data_path.is_file():
                    continue

                data = read_csv(data_path).replace({nan: None})
                data_as_records = data.to_dict(orient='records')

                try:
                    validate_data(
                        data_as_records, schema=DATA_SCHEMAS.get(filename, {})
                    )
                    data_rows_to_db(
                        session, data=data, data_source=str(data_path), log=log
                    )
                except:
                    log.exception(
                        f'The data in {data_path} could not be added to the database.'
                    )
                    sys_exit(1)
                else:
                    added_data_sources.append(str(data_path))

        if not sync_with_gdrive:
            log.info(
                f'Successfuly added data from {added_data_sources} to the database.'
            )
            return

        self.sync_with_gdrive()

    def sync_with_gdrive(self):
        with self._Session.begin() as session:
            stmt = select(Platform)
            platforms = session.execute(stmt).scalars().all()

            for platform in platforms:
                platform_spec_path = (
                    self._platform_tracking_sheet_specs_dir / f'{platform.name}.yml'
                )

                if not platform_spec_path.exists():
                    log.warning(
                        f'{platform_spec_path} not found. Skipping ingestion of {platform.name} data from Google Drive.'
                    )
                    continue

                try:
                    platform_spec = safe_load(platform_spec_path.read_text())
                    validate_data(platform_spec, schema=GDRIVE_PLATFORM_SPEC_SCHEMA)
                except:
                    log.exception(
                        f'Error reading or validating {platform_spec_path}. Skipping ingestion of {platform.name} data from Google Drive.'
                    )
                    continue

                spreadsheet = self._gclient.open_by_url(
                    platform_spec['spreadsheet_url']
                )

                main_sheet_id = platform_spec['main_sheet_id']
                main_sheet = spreadsheet.get_worksheet_by_id(main_sheet_id)
                main_sheet_spec = platform_spec['worksheets'][main_sheet_id]

                data_source = (
                    f'{spreadsheet.title} - {main_sheet.title} ({main_sheet.url})'
                )
                try:
                    datas = TrackingSheet(
                        worksheet=main_sheet, **main_sheet_spec
                    ).to_dfs()
                except:
                    logging.exception(
                        f'Error reading data from {data_source}. Skipping ingestion of {platform.name} data from Google Drive.'
                    )
                    continue

                for model_name in self._data_insertion_order:
                    if model_name not in datas:
                        continue
                    try:
                        data_rows_to_db(
                            session, datas[model_name], data_source=data_source, log=log
                        )
                    except:
                        log.exception(
                            f'{model_name} data from {data_source} could not be added to the database.'
                        )

    def delivery_metrics_to_gdrive(self, pipeline_output_dir: DirectoryPath):
        try:
            raise NotImplementedError
        except:
            log.exception('')


def main():
    fire.Fire(SCBLUtils)
