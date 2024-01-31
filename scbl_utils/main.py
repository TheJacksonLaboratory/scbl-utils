"""
This module contains the command-line interface for the `scbl-utils`
package. It allows users to call subcommands and initialize a database,
containing information about labs and institutions.

Functions:
    - init_db: Initialize the database with institutions and labs
"""
# TODO list in order of priority
# TODO remove unused imports
from pathlib import Path
from typing import Annotated

import gspread as gs
import typer
from rich import print as rprint
from sqlalchemy import select

from .core.data_io import load_data
from .core.db import data_rows_to_db, db_session
from .core.gdrive import TrackingSheet
from .core.validation import validate_dir
from .db_models.base import Base
from .db_models.data_models.chromium import Library, LibraryType, SequencingRun, Tag
from .db_models.metadata_models import (
    DataSet,
    Institution,
    Lab,
    Person,
    Platform,
    Project,
    Sample,
)
from .defaults import (
    CONFIG_DIR,
    DATA_INSERTION_ORDER,
    DATA_SCHEMAS,
    DB_CONFIG_FILES,
    DB_INIT_FILES,
    DB_SPEC_SCHEMA,
    GDRIVE_CONFIG_FILES,
    GDRIVE_PLATFORM_SPEC_SCHEMA,
    SIBLING_REPOSITORY,
)

app = typer.Typer()


@app.callback(no_args_is_help=True)
def callback(
    config_dir: Annotated[
        Path,
        typer.Option(
            '--config-dir',
            '-c',
            help=(
                'Configuration directory containing files necessary for the '
                'script to run.'
            ),
        ),
    ] = CONFIG_DIR
) -> None:
    """
    Command-line utilities that facilitate data processing in the
    Single Cell Biology Lab at the Jackson Laboratory.
    """
    global CONFIG_DIR
    CONFIG_DIR = config_dir
    validate_dir(config_dir)


@app.command(no_args_is_help=True)
def init_db(
    data_dir: Annotated[
        Path,
        typer.Argument(
            help='Path to a directory containing the data necessary to '
            'initialize the database. This directory must contain the '
            'following files: ' + ', '.join(path.name for path in DB_INIT_FILES)
        ),
    ],
):
    """
    Initialize the database with the institutions, labs, people,
    platforms, library types, and tags.
    """
    db_config_dir = CONFIG_DIR / 'db'
    config_files = validate_dir(db_config_dir, required_files=DB_CONFIG_FILES)
    spec: dict = load_data(config_files['db-spec.yml'], schema=DB_SPEC_SCHEMA)

    data_files = validate_dir(data_dir, required_files=DB_INIT_FILES)
    datas = {
        path.stem: load_data(path, schema=DATA_SCHEMAS[path.stem])
        for path in data_files.values()
    }

    ordered_model_names = (
        model_name for model_name in DATA_INSERTION_ORDER if model_name in datas
    )

    Session = db_session(base_class=Base, **spec)
    for model_name in ordered_model_names:
        with Session.begin() as session:
            data_rows_to_db(session, datas[model_name], data_source=f'{model_name}.csv')


@app.command()
def sync_db_with_gdrive():
    """ """
    db_config_dir = CONFIG_DIR / 'db'
    db_config_files = validate_dir(db_config_dir, required_files=DB_CONFIG_FILES)
    db_spec: dict = load_data(db_config_files['db-spec.yml'], schema=DB_SPEC_SCHEMA)

    gdrive_config_dir = CONFIG_DIR / 'google-drive'
    gdrive_config_files = validate_dir(
        gdrive_config_dir, required_files=GDRIVE_CONFIG_FILES
    )

    gclient = gs.service_account(filename=gdrive_config_files['service-account.json'])

    Session = db_session(base_class=Base, **db_spec)
    with Session.begin() as session:
        stmt = select(Platform)
        platforms = session.execute(stmt).scalars().all()

    platform_spec_dir = gdrive_config_files['gdrive_platform_specs']
    for platform in platforms:
        platform_spec_path = platform_spec_dir / f'{platform.name}_spec.yml'

        if not platform_spec_path.exists():
            rprint(
                f'[green]{platform_spec_path.name}[/] not found in [orange1]{platform_spec_dir}[/]. Skipping ingestion of [green]{platform.name}[/] data from Google Drive.'
            )
            continue

        gdrive_spec: dict = load_data(
            platform_spec_path, schema=GDRIVE_PLATFORM_SPEC_SCHEMA
        )

        tracking_spreadsheet = gclient.open_by_url(gdrive_spec['spreadsheet_url'])

        main_sheet_id = gdrive_spec['main_sheet_id']
        main_sheet = tracking_spreadsheet.get_worksheet_by_id(main_sheet_id)
        main_sheet_spec = gdrive_spec['worksheets'][main_sheet_id]

        main_datas = TrackingSheet(worksheet=main_sheet, **main_sheet_spec).to_dfs()
        data_source = (
            f'{tracking_spreadsheet.title} - {main_sheet.title} ({main_sheet.url})'
        )

        ordered_tables = (
            table for table in DATA_INSERTION_ORDER if table in main_datas
        )
        for tablename in ordered_tables:
            with Session.begin() as session:
                data_rows_to_db(session, main_datas[tablename], data_source=data_source)
