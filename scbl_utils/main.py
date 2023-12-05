"""
This module contains the command-line interface for the `scbl-utils`
package. It allows users to call subcommands and initialize a database,
containing information about labs and institutions.

Functions:
    - init_db: Initialize the database with institutions and labs
"""
# TODO write a script that parses the delivery dir for new labs, adding
# to the db and prompting user if necessary
from pathlib import Path
from typing import Annotated

import typer
from sqlalchemy import select, func

from .core import load_data, new_db_session, validate_dir, matching_rows_from_table
from .db_models.bases import Base
from .db_models.data import Institution, Lab, LibraryType, Person, Platform, Tag
from .defaults import (
    CONFIG_DIR,
    CSV_SCHEMAS,
    DB_CONFIG_FILES,
    DB_INIT_FILES,
    SPEC_SCHEMA,
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
            'following files:\n'
            + '\n'.join(path.name for path in DB_INIT_FILES.values())
        ),
    ],
):
    """
    Initialize the database with the institutions, labs, people, platforms, library types, and tags.
    """
    db_config_dir = CONFIG_DIR / 'db'
    config_files = validate_dir(db_config_dir, required_files=DB_CONFIG_FILES)
    spec: dict = load_data(config_files['db-spec.yml'], schema=SPEC_SCHEMA)

    data_files = validate_dir(data_dir, required_files=DB_INIT_FILES.values())
    data = {
        filename: load_data(path, schema=CSV_SCHEMAS[filename])
        for filename, path in data_files.items()
    }

    csv_to_model = {
        'institution.csv': Institution,
        'lab.csv': Lab,
        'person.csv': Person,
        'platform.csv': Platform,
        'librarytype.csv': LibraryType,
        'tag.csv': Tag
        }
    
    initial_data = {
        filename: [csv_to_model[filename](**row) for row in dataset]
        for filename, dataset in data.items()
        if filename != 'lab.csv'
    }

    Session = new_db_session(base_class=Base, **spec)
    with Session.begin() as session:
        for object_list in initial_data.values():
            session.add_all(object_list)

    with Session.begin() as session:
        filter_dicts = [
            {'name': lab_row['institution_name']} for lab_row in data['lab.csv']
        ]
        lab_institutions = matching_rows_from_table(
            session,
            model=Institution,
            filter_dicts=filter_dicts,
            data_filename='lab.csv',
        )

        filter_dicts = [
            {
                'first_name': lab_row['pi_first_name'],
                'last_name': lab_row['pi_last_name'],
                'email': lab_row['pi_email'],
                'orcid': lab_row['pi_orcid'],
            }
            for lab_row in data['lab.csv']
        ]
        lab_pis = matching_rows_from_table(
            session, model=Person, filter_dicts=filter_dicts, data_filename='lab.csv'
        )

        labs = [Lab(institution=institution, pi=pi, name=lab_row['name'], delivery_dir=lab_row['delivery_dir']) for institution, pi, lab_row in zip(lab_institutions, lab_pis, data['lab.csv'])]  # type: ignore
        session.add_all(labs)
