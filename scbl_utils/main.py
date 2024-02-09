from os import environ
from pathlib import Path
from shutil import copytree

import fire
import gspread as gs
import pandas
import pydantic
from jsonschema import validate as validate_data
from numpy import nan
from pydantic.dataclasses import dataclass
from rich.console import Console
from rich.traceback import install
from sqlalchemy import select
from yaml import safe_load

from .db.core import data_rows_to_db, db_session, get_matching_obj
from .db.helpers import date_to_id
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

console = Console()
install(console=console, max_frames=1, suppress=[fire, pandas, pydantic])
pandas.set_option('future.no_silent_downcasting', True)


@dataclass(kw_only=True)
class SCBLUtils(object):
    """A set of command-line utilities that facilitate data processing at the Single Cell Biology Lab at the Jackson Laboratory."""

    config_dir: pydantic.DirectoryPath = pydantic.Field(
        default='/sc/service/etc/.config/scbl-utils', validate_default=True
    )
    _data_insertion_order: list[str] = pydantic.Field(
        init=False,
        default=[
            'Institution',
            'Person',
            'Lab',
            'Platform',
            'ChromiumAssay',
            'ChromiumLibraryType',
            'ChromiumTag',
            'SequencingRun',
            'ChromiumDataSet',
            'ChromiumLibrary',
            'ChromiumSample',
            'XeniumRun',
            'XeniumDataSet',
            'XeniumSample',
        ],
        validate_default=True,
    )

    @pydantic.field_validator('config_dir', mode='before')
    @classmethod
    def _convert_config_dir(cls, config_dir: str) -> Path:
        return Path(config_dir).expanduser()

    @pydantic.field_validator('config_dir', mode='after')
    @classmethod
    def _validate_config_dir(
        cls, config_dir: pydantic.DirectoryPath
    ) -> pydantic.DirectoryPath:
        required_files = {
            'db_spec': 'db/db_spec.yml',
            'gdrive_credentials': 'google-drive/service-account.json',
            'system_config': 'system/config.yml',
        }
        required_directories = {
            'platform_tracking-sheet_specs': 'google-drive/platform_tracking-sheet_specs'
        }
        validate_directory(
            config_dir,
            required_files=required_files.values(),
            required_directories=required_directories.values(),
        )
        return config_dir

    @pydantic.field_validator('_data_insertion_order', mode='after')
    @classmethod
    def _validate_data_insertion_order(
        cls, data_insertion_order: list[str]
    ) -> list[str]:
        if missing_models := set(data_insertion_order) - {
            model.class_.__name__ for model in Base.registry.mappers
        }:
            raise ValueError(
                f'The data insertion order must include all models in the database. The following are missing: {missing_models}'
            )

        return data_insertion_order

    @pydantic.model_validator(mode='after')
    def _load_config(self) -> 'SCBLUtils':
        self._platform_tracking_sheet_specs_dir = Path(
            self.config_dir, 'google-drive', 'platform_tracking-sheet_specs'
        )

        db_spec_path = Path(self.config_dir, 'db', 'db_spec.yml')
        db_spec: dict[str, str] = safe_load(db_spec_path.read_text())
        validate_data(db_spec, schema=DB_SPEC_SCHEMA)
        self._Session = db_session(Base, **db_spec)
        self._db_spec = db_spec

        gdrive_credential_path = Path(
            self.config_dir, 'google-drive', 'service-account.json'
        )
        self._gclient = gs.service_account(filename=gdrive_credential_path)

        system_config_path = Path(self.config_dir, 'system', 'config.yml')
        system_config: dict[str, str] = safe_load(system_config_path.read_text())
        validate_data(system_config, schema=SYSTEM_CONFIG_SCHEMA)
        environ.update(system_config)
        self._system_config = system_config

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

                data = pandas.read_csv(data_path).replace({nan: None})
                data_as_records = data.to_dict(orient='records')

                validate_data(data_as_records, schema=DATA_SCHEMAS.get(filename, {}))
                data_rows_to_db(
                    session, data=data, data_source=str(data_path), console=console
                )
                added_data_sources.append(str(data_path))

        if not sync_with_gdrive:
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
                    console.print(
                        f'{platform_spec_path} not found. Skipping ingestion of [green]{platform.name}[/] data from Google Drive.',
                        end='\n\n',
                    )
                    continue

                platform_spec = safe_load(platform_spec_path.read_text())
                validate_data(platform_spec, schema=GDRIVE_PLATFORM_SPEC_SCHEMA)

                spreadsheet = self._gclient.open_by_url(
                    platform_spec['spreadsheet_url']
                )

                main_sheet_id = platform_spec['main_sheet_id']
                main_sheet = spreadsheet.get_worksheet_by_id(main_sheet_id)
                main_sheet_spec = platform_spec['worksheets'][main_sheet_id]

                data_source = (
                    f'{spreadsheet.title} - {main_sheet.title} ({main_sheet.url})'
                )

                datas = TrackingSheet(worksheet=main_sheet, **main_sheet_spec).to_dfs()

                for model_name in self._data_insertion_order:
                    if model_name not in datas:
                        continue

                    data = datas[model_name]

                    model = next(
                        model.class_
                        for model in Base.registry.mappers
                        if model.class_.__name__ == model_name
                    )
                    # TODO: implement something neater that figures out whether the model requires an ID and then assign it if it's not there
                    if not issubclass(model, (DataSet, Sample)):
                        try:
                            data_rows_to_db(
                                session,
                                data,
                                data_source=data_source,
                                console=console,
                            )
                        except Exception as e:
                            console.print(str(e), end='\n\n')

                        continue

                    data[f'{model_name}.platform.name'] = platform.name

                    if issubclass(model, DataSet):
                        prefix = platform.data_set_id_prefix
                        id_length = platform.data_set_id_length
                        date_column = f'{model_name}.date_initialized'
                    else:
                        prefix = platform.sample_id_prefix
                        id_length = platform.sample_id_length
                        date_column = f'{model_name}.date_received'

                    data[f'{model_name}.id'] = data[[date_column]].apply(
                        date_to_id, prefix=prefix, id_length=id_length, axis=1
                    )

                    try:
                        data_rows_to_db(
                            session,
                            data,
                            data_source=data_source,
                            console=console,
                        )
                    except Exception as e:
                        console.print(str(e), end='\n\n')

    # TODO: make this function more flexible for other assays/platforms
    # TODO: in the configuration, there should be a platform-specific
    # of handling directories, with regex and structures predefined in
    # self._config/something.yml. Also, this just needs to be cleaned
    @pydantic.validate_call(config=pydantic.ConfigDict(validate_default=True))
    def format_xenium_dir(
        self,
        xenium_dir: pydantic.DirectoryPath,
        output_dir: pydantic.DirectoryPath = Path('/sc/service/staging'),
    ):
        slide_serial_numbers = {
            sample_dir.name.split('__')[1] for sample_dir in xenium_dir.iterdir()
        }  # TODO: change this when the xenium ID is corrected
        with self._Session.begin() as session:
            for serial_number in slide_serial_numbers:
                match_on = {'slide_serial_number': serial_number}
                data_set = get_matching_obj(
                    data=match_on, model=XeniumDataSet, session=session
                )

                # TODO: cleaner error handling
                if not isinstance(data_set, XeniumDataSet):
                    raise ValueError(
                        f'No data set found for slide ID [orange1]{serial_number}[/]'
                    )

                staging_dir = output_dir / Path(data_set.lab.delivery_dir).parts[-1]
                slide_dir = staging_dir / f'{serial_number}_{data_set.slide_name}'

                sub_dirs = ['design', 'slide_img', 'regions']

                sample_id_to_dir = {
                    dir_.name[2]: dir_
                    for dir_ in xenium_dir.glob(f'*__{serial_number}__*')
                }
                for sample_id, dir_ in sample_id_to_dir.items():
                    for sub_dir in sub_dirs:
                        (slide_dir / sub_dir).mkdir(parents=True, exist_ok=True)

                        if sub_dir == 'regions':
                            sample = next(
                                sample
                                for sample in data_set.samples
                                if sample.id == sample_id
                            )
                            copytree(
                                src=dir_,
                                dst=slide_dir / sub_dir / f'{sample_id}_{sample.name}',
                            )

    def delivery_metrics_to_gdrive(self, pipeline_output_dir: pydantic.DirectoryPath):
        raise NotImplementedError


def main():
    fire.Fire(SCBLUtils)
