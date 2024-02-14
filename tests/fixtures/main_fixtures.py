from json import dump as dump_json
from json import loads
from os import getenv
from pathlib import Path

from pytest import exit as test_exit
from pytest import fixture
from yaml import dump as dump_yml

from .db.data import delivery_parent_dir
from .db.utils import tmp_db_path


# TODO: finish this
@fixture
def config_dir(tmp_path: Path, tmp_db_path: Path, delivery_parent_dir: Path) -> Path:
    """
    Create a temporary configuration directory for testing.
    """
    config_dir = tmp_path / '.config'
    config_sub_dirs = ['db', 'google-drive', 'system']
    for sub_dir in config_sub_dirs:
        (config_dir / sub_dir).mkdir(parents=True)

    db_spec = {'drivername': 'sqlite', 'database': str(tmp_db_path)}
    with (config_dir / 'db' / 'db_spec.yml').open('w') as f:
        dump_yml(db_spec, stream=f)

    service_account_credential_path = getenv('GOOGLE-DRIVE-CREDENTIALS')
    service_account_credential_path = (
        Path(service_account_credential_path)
        if service_account_credential_path is not None
        else None
    )
    if service_account_credential_path is None:
        test_exit(
            'GOOGLE-DRIVE-CREDENTIALS environment variable not set. Set to the path of the service account credentials JSON file.',
            returncode=1,
        )

    credentials = loads(service_account_credential_path.read_text())
    with (config_dir / 'google-drive' / 'service-account.json').open('w') as f:
        dump_json(credentials, f)

    system_config = {'delivery_parent_dir': delivery_parent_dir}
    with (config_dir / 'system' / 'config.yml').open('w') as f:
        dump_yml(system_config, stream=f)

    return config_dir.parent
