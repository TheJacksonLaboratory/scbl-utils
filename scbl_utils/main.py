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

from .core import load_data, validate_dir, new_db_session
from .defaults import DB_CONFIG_FILES, INSTITUION_CSV_SCHEMA, LAB_CSV_SCHEMA, SPEC_SCHEMA
from .db_models.bases import Base
from .db_models.data import Institution, Lab, Person

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
    institutions_data_path: Annotated[
        Path,
        typer.Argument(
            help=(
                'A CSV containing information about the institutions used to '
                'initialize the database.'
            )
        ),
    ],
    labs_data_path: Annotated[
        Path,
        typer.Argument(
            help=(
                'A CSV containing information about the labs used to '
                'initialize the database.'
            )
        ),
    ],
):
    """
    Initialize the database with the given institutions and labs.
    """
    db_config_dir = CONFIG_DIR / 'db'
    config_files = validate_dir(db_config_dir, required_files=DB_CONFIG_FILES)
    spec: dict = load_data(config_files['db-spec.yml'], schema=SPEC_SCHEMA)

    validate_dir(institutions_data_path.parent, required_files=[institutions_data_path])
    institutions_data: list = load_data(
        institutions_data_path, schema=INSTITUION_CSV_SCHEMA
    )

    validate_dir(labs_data_path.parent, required_files=[labs_data_path])
    labs_data: list = load_data(labs_data_path, schema=LAB_CSV_SCHEMA)

    institution_list = [Institution(**row) for row in institutions_data]
    institutions = {
        institution.short_name: institution for institution in institution_list
    }

    pi_set = {
        (
            lab_row['pi_first_name'].lower(),
            lab_row['pi_last_name'].lower(),
            lab_row['pi_email'].lower() if lab_row['pi_email'] is not None else None,
            lab_row['pi_orcid'].lower() if lab_row['pi_orcid'] is not None else None,
        )
        for lab_row in labs_data
    }
    pis = {pi_def: Person(*pi_def) for pi_def in pi_set}

    labs = [
        Lab(
            pi=pis[
                (
                    lab_row['pi_first_name'].lower(),
                    lab_row['pi_last_name'].lower(),
                    lab_row['pi_email'].lower()
                    if lab_row['pi_email'] is not None
                    else None,
                    lab_row['pi_orcid'].lower()
                    if lab_row['pi_orcid'] is not None
                    else None,
                )
            ],
            institution=institutions[lab_row['institution_short_name']],
            delivery_dir=lab_row['delivery_dir'],
            name=lab_row['name'],
        )
        for lab_row in labs_data
    ]

    Session = new_db_session(base_class=Base, **spec)
    with Session.begin() as session:
        session.add_all(labs)
