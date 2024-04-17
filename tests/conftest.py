import json
from os import environ
from pathlib import Path
from typing import Any

import dotenv
import pydantic
from pytest import MonkeyPatch, fixture
from yaml import safe_dump

from scbl_utils.config import (
    DBConfig,
    GoogleColumnConfig,
    GoogleSpreadsheetConfig,
    GoogleWorksheetConfig,
    MergeStrategy,
    SystemConfig,
)


@fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / 'tmp.db'


@fixture
def db_config(db_path: Path) -> DBConfig:
    return DBConfig(database=str(db_path))


@fixture
def system_config(monkeypatch: MonkeyPatch, tmp_path: Path) -> SystemConfig:
    delivery_parent_dir = tmp_path / 'delivery'
    delivery_parent_dir.mkdir()

    config = SystemConfig(delivery_parent_dir=delivery_parent_dir)

    monkeypatch.setenv('delivery_parent_dir', str(delivery_parent_dir))
    monkeypatch.setattr('pathlib.Path.group', lambda _: 'test_group')
    monkeypatch.setattr('pathlib.Path.is_dir', lambda _: True)

    return config


@fixture
def google_column_configs() -> dict[str, GoogleColumnConfig]:
    return {
        'ROR ID': GoogleColumnConfig(targets={'Institution.ror_id'}),
        'Email Format': GoogleColumnConfig(targets={'Institution.email_format'}),
    }


@fixture
def google_worksheet_configs(
    google_column_configs: dict[str, GoogleColumnConfig]
) -> dict[str, GoogleWorksheetConfig]:
    return {
        'Institution': GoogleWorksheetConfig(column_to_targets=google_column_configs)
    }


@fixture
def google_spreadsheet_config(
    google_worksheet_configs: dict[str, GoogleWorksheetConfig]
) -> GoogleSpreadsheetConfig:
    return GoogleSpreadsheetConfig(
        spreadsheet_id='1l1-YLW6M6PAOLbi_QrLRsLW3obgIX69gKjEiM9pRoFg',
        worksheet_configs=google_worksheet_configs,
        merge_strategies={
            'Institution': MergeStrategy(
                merge_on='Institution.name', order=['Institution']
            )
        },
    )


@fixture
def google_drive_credentials() -> Any:
    dotenv.load_dotenv()
    raw_credential_string = Path(environ['GOOGLE_DRIVE_CREDENTIAL_PATH']).read_text()

    return json.loads(raw_credential_string)


@fixture
def config_dir(
    db_config: DBConfig,
    system_config: SystemConfig,
    google_spreadsheet_config: GoogleSpreadsheetConfig,
    google_drive_credentials: Any,
    tmp_path: Path,
) -> Path:
    config_directory = tmp_path / '.config'

    sub_config_directories = [config_directory / 'google-drive' / 'tracking_sheets']
    for dir_ in sub_config_directories:
        dir_.mkdir(parents=True)

    filename_to_data = (
        ('db.yml', db_config),
        ('system.yml', system_config),
        ('google-drive/service-account.json', google_drive_credentials),
        ('google-drive/tracking_sheets/Institution.yml', google_spreadsheet_config),
    )

    for fname, data in filename_to_data:
        with (config_directory / fname).open('w') as f:
            if isinstance(data, pydantic.BaseModel):
                safe_dump(data=data.model_dump(mode='json'), stream=f)

            else:
                json.dump(data, fp=f)

    return config_directory
