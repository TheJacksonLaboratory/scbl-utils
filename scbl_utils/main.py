from functools import cached_property
from os import environ
from pathlib import Path
from shutil import copytree

import fire
import gspread as gs
import pandas as pd
import pydantic
from jsonschema import validate as validate_schema
from pydantic.dataclasses import dataclass
from rich.console import Console
from rich.traceback import install
from sqlalchemy.orm import Session, sessionmaker
from yaml import safe_load

from .data_io.validators import validate_directory
from .db.core import assign_ids, data_rows_to_db, db_session
from .db.helpers import get_matching_obj
from .db.orm.base import Base
from .db.orm.models.data import Platform
from .db.orm.models.platforms.chromium import (
    ChromiumAssay,
    ChromiumDataSet,
    ChromiumLibrary,
    ChromiumLibraryType,
    ChromiumSample,
    ChromiumTag,
    SequencingRun,
)
from .db.orm.models.platforms.xenium import XeniumDataSet, XeniumRun, XeniumSample
from .db.validators import validate_data_columns
from .gdrive.core import TrackingSheet
from .json_schemas.config_schemas import (
    DB_SPEC_SCHEMA,
    GDRIVE_PLATFORM_SPEC_SCHEMA,
    SYSTEM_CONFIG_SCHEMA,
)

console = Console()
install(console=console, max_frames=1, suppress=[fire, pd, pydantic])
pd.set_option('future.no_silent_downcasting', True)


@dataclass(kw_only=True, config=pydantic.ConfigDict(arbitrary_types_allowed=True))
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
        frozen=True,
    )

    @pydantic.field_validator('config_dir', mode='after')
    @classmethod
    def _validate_config_dir(
        cls, config_dir: pydantic.DirectoryPath
    ) -> pydantic.DirectoryPath:
        config_structure = {
            'db': ['db_spec.yml'],
            'google-drive': {
                'platform_tracking-sheet_specs': [],
                '.': ['service-account.json'],
            },
            'system': ['config.yml'],
        }
        validate_directory(config_dir, required_structure=config_structure)

        return config_dir

    @pydantic.computed_field(repr=False)
    @cached_property
    def _db_session(self: 'SCBLUtils') -> sessionmaker[Session]:
        spec_path = self.config_dir / 'db' / 'db_spec.yml'
        spec = safe_load(spec_path.read_bytes())

        validate_schema(spec, schema=DB_SPEC_SCHEMA)
        return db_session(Base, **spec)

    @pydantic.computed_field(repr=False)
    @cached_property
    def _gdrive_config_dir(self: 'SCBLUtils') -> Path:
        return self.config_dir / 'google-drive'

    @pydantic.computed_field(repr=False)
    @cached_property
    def _gclient(self: 'SCBLUtils') -> gs.Client:
        credential_path = self._gdrive_config_dir / 'service-account.json'
        return gs.service_account(filename=credential_path)

    # TODO: must test that these are a subset of data_insertion_order
    @pydantic.computed_field(repr=False)
    @cached_property
    def _model_to_id_date_column(self: 'SCBLUtils') -> dict[type, str]:
        return {DataSet: 'date_initialized', Sample: 'date_received'}

    @pydantic.computed_field(repr=False)
    @cached_property
    def _platform_tracking_sheet_specs(self: 'SCBLUtils') -> dict[str, dict]:
        platform_tracking_sheet_spec_dir = (
            self._gdrive_config_dir / 'platform_tracking-sheet_specs'
        )
        specs = {
            spec_path.stem: safe_load(spec_path.read_bytes())
            for spec_path in platform_tracking_sheet_spec_dir.iterdir()
        }
        for spec in specs.values():
            validate_schema(spec, schema=GDRIVE_PLATFORM_SPEC_SCHEMA)

        return specs

    @pydantic.computed_field(repr=False)
    @cached_property
    def _system_config(self: 'SCBLUtils') -> dict[str, str]:
        spec_path = self.config_dir / 'system' / 'config.yml'
        spec = safe_load(spec_path.read_bytes())

        validate_schema(spec, schema=SYSTEM_CONFIG_SCHEMA)
        environ.update(spec)
        return spec

    @pydantic.validate_call
    def fill_db(self, data_dir: pydantic.DirectoryPath | None = None) -> None:
        if data_dir is not None:
            self._directory_to_db(data_dir)

        self._gdrive_to_db()

    def _directory_to_db(self, data_dir: pydantic.DirectoryPath):
        validated_datas = {}
        for model_name in self._data_insertion_order:
            data_path = data_dir / f'{model_name}.csv'

            if not data_path.is_file():
                continue

            data = pd.read_csv(data_path)
            validate_data_columns(
                data.columns, db_model_base_class=Base, data_source=str(data_path)
            )

            validated_datas[model_name] = data

        with self._db_session.begin() as session:
            validated_datas = assign_ids(
                validated_datas, session=session, db_base_class=Base
            )

        with self._db_session.begin() as session:
            for model_name, data in validated_datas.items():
                data_rows_to_db(
                    session=session,
                    data=data,
                    data_source=f'{model_name}.csv',
                    console=console,
                    db_base_class=Base,
                )

    def _gdrive_to_db(self):
        for platform_name, platform_spec in self._platform_tracking_sheet_specs.items():
            spreadsheet = self._gclient.open_by_url(platform_spec['spreadsheet_url'])
            main_sheet = spreadsheet.get_worksheet_by_id(platform_spec['main_sheet_id'])
            main_sheet_spec = platform_spec['worksheets'][main_sheet.id]

            data_source = f'{spreadsheet.title} - {main_sheet.title} ({main_sheet.url})'
            datas = TrackingSheet(worksheet=main_sheet, **main_sheet_spec).to_dfs()

            validated_datas = {}
            for model_name, data in datas.items():
                data[f'{model_name}.platform.name'] = platform_name
                validate_data_columns(
                    data.columns, db_model_base_class=Base, data_source=data_source
                )

                validated_datas[model_name] = data.copy()

            with self._db_session.begin() as session:
                validated_datas = assign_ids(
                    validated_datas, db_base_class=Base, session=session
                )

            with self._db_session.begin() as session:
                for model_name, data in validated_datas.items():
                    data_rows_to_db(
                        session=session,
                        data=data,
                        data_source=data_source,
                        console=console,
                        db_base_class=Base,
                    )

        pass

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
        raise NotImplementedError
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
