from pathlib import Path
from typing import Any

from pytest import MonkeyPatch, fixture
from yaml import safe_dump

from scbl_utils.config_models.db import DBConfig
from scbl_utils.config_models.system import SystemConfig


@fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / 'tmp.db'


@fixture
def db_config(db_path: Path) -> dict[str, Any]:
    return DBConfig(database=str(db_path)).model_dump(mode='json')


@fixture
def system_config(monkeypatch: MonkeyPatch, tmp_path: Path) -> dict[str, Any]:
    delivery_parent_dir = tmp_path / 'delivery'
    delivery_parent_dir.mkdir()

    config = SystemConfig(delivery_parent_dir=delivery_parent_dir).model_dump(
        mode='json'
    )

    monkeypatch.setenv('delivery_parent_dir', str(delivery_parent_dir))
    monkeypatch.setattr('pathlib.Path.group', lambda _: 'test_group')
    monkeypatch.setattr('pathlib.Path.is_dir', lambda _: True)

    return config


@fixture
def config_dir(
    db_config: dict[str, Any], system_config: dict[str, Any], tmp_path: Path
) -> Path:
    directory = tmp_path / '.config'
    directory.mkdir()

    filename_to_data = (('db.yml', db_config), ('system.yml', system_config))

    for fname, data in filename_to_data:
        with (directory / fname).open('w') as f:
            safe_dump(data=data, stream=f)

    return directory
