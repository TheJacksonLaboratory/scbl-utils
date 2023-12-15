"""
This module contains the command-line interface for the `scbl-utils`
package. It allows users to call subcommands and initialize a database,
containing information about labs and institutions.

Functions:
    - init_db: Initialize the database with institutions and labs
"""
# TODO: write comments for init-db
from pathlib import Path
from typing import Annotated

import gspread as gs
import typer

from .core.data_io import load_data
from .core.db import db_session, matching_rows_from_table
from .core.validation import validate_dir
from .db_models.bases import Base
from .db_models.data import (
    DataSet,
    Institution,
    Lab,
    Library,
    LibraryType,
    Person,
    Platform,
    Sample,
    SequencingRun,
    Tag,
)
from .defaults import (
    CONFIG_DIR,
    CSV_SCHEMAS,
    DB_CONFIG_FILES,
    DB_INIT_FILES,
    DB_SPEC_SCHEMA,
    GDRIVE_CONFIG_FILES,
    GDRIVE_SPEC_SCHEMA,
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


@app.command()
def init_db(
    data_dir: Annotated[
        Path,
        typer.Argument(
            help='Path to a directory containing the data necessary to '
            'initialize the database. This directory must contain the '
            'following files:\n' + '\n'.join(path.name for path in DB_INIT_FILES)
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
    data = {
        filename: load_data(path, schema=CSV_SCHEMAS[filename])
        for filename, path in data_files.items()
    }

    csv_to_model = {
        f'{model.__tablename__}.csv': model for model in Base.__subclasses__()
    }

    initial_data = {
        filename: [csv_to_model[filename](**row) for row in dataset]
        for filename, dataset in data.items()
        if filename != 'lab.csv'
    }

    Session = db_session(base_class=Base, **spec)
    with Session.begin() as session:
        for object_list in initial_data.values():
            session.add_all(object_list)

    with Session.begin() as session:
        institution_att_to_csv_col = {'name': 'institution_name'}
        lab_institutions = matching_rows_from_table(
            session,
            Institution,
            institution_att_to_csv_col,
            data=data['lab.csv'],
            data_filename='lab.csv',
        )

        person_att_to_csv_col = {
            'first_name': 'pi_first_name',
            'last_name': 'pi_last_name',
            'email': 'pi_email',
            'orcid': 'pi_orcid',
        }
        lab_pis = matching_rows_from_table(
            session,
            Person,
            person_att_to_csv_col,
            data=data['lab.csv'],
            data_filename='lab.csv',
        )

        labs = [
            Lab(
                institution=institution,
                pi=pi,
                name=lab_row['name'],
                delivery_dir=lab_row['delivery_dir'],
            )
            for institution, pi, lab_row in zip(
                lab_institutions, lab_pis, data['lab.csv']
            )
        ]
        session.add_all(labs)


# @app.command()
# def sync_db_with_gdrive():
#     f"""
#     Generate a samplesheet to use as input to the nf-tenx
#     ({SIBLING_REPOSITORY}) pipeline.
#     """
#     db_config_dir = CONFIG_DIR / 'db'
#     db_config_files = validate_dir(db_config_dir, required_files=DB_CONFIG_FILES)
#     db_spec: dict = load_data(db_config_files['db-spec.yml'], schema=DB_SPEC_SCHEMA)

#     gdrive_config_dir = CONFIG_DIR / 'google-drive'
#     gdrive_config_files = validate_dir(
#         gdrive_config_dir, required_files=GDRIVE_CONFIG_FILES
#     )
#     gdrive_spec: dict = load_data(
#         gdrive_config_files['gdrive-spec.yml'], schema=GDRIVE_SPEC_SCHEMA
#     )


#     gclient = gs.service_account(filename=gdrive_config_files['service-account.json'])
#     tracking_sheet = gclient.open_by_url(gdrive_spec['spreadsheet_url'])

#     tracking_sheet
#     for worksheet in gdrive_spec['worksheets']:
#         records = somefunc(
#             tracking_sheet,
#             worksheet_id=worksheet['id'],
#             columns=worksheet['columns'],
#             head=worksheet['head'],
#         )
#         data = multi_table_records_to_models(
#             records,
#             models=[
#                 Institution,
#                 Lab,
#                 Person,
#                 Platform,
#                 LibraryType,
#                 Tag,
#                 Library,
#                 Experiment,
#                 Sample,
#                 SequencingRun,
#             ],
#         )

#     pass
