from pathlib import Path

import pytest

from scbl_utils.utils import validate
from scbl_utils.utils.defaults import GDRIVE_CONFIG_FILES
from scbl_utils.utils.gdrive import load_specs


@pytest.fixture(scope='session', autouse=True)
def dirs() -> dict[str, Path]:
    working_dir = Path(__file__).parent.absolute()
    return {
        path.name: path.absolute() for path in working_dir.iterdir() if path.is_dir()
    }


@pytest.fixture(scope='session')
def config_dir():
    return Path.home() / '.config' / 'test-scbl-utils'  # TODO: change to real path


@pytest.fixture(scope='session')
def gdrive_config_files(config_dir) -> dict[str, Path]:
    gdrive_config_dir = config_dir / 'google-drive'
    return validate.direc(gdrive_config_dir, required_files=GDRIVE_CONFIG_FILES)


@pytest.fixture(scope='session')
def specs(gdrive_config_files) -> tuple[dict, dict]:
    return load_specs(gdrive_config_files)


@pytest.fixture
def tracking_spec(specs) -> dict:
    return specs[0]


@pytest.fixture
def metrics_spec(specs) -> dict:
    return specs[1]
