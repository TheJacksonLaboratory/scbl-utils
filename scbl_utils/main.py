"""
This module contains the command-line interface for the `scbl-utils`
package. It allows users to call subcommands and initialize a database,
containing information about labs and institutions.

Functions:
    - init_db: Initialize the database with institutions and labs
"""
# TODO: write comments for init-db
# TODO: go through and make distinction between models and tables
from pathlib import Path
from typing import Annotated

import gspread as gs
import typer
from rich import print as rprint
from rich.prompt import Prompt
from sqlalchemy import select

from .core.data_io import load_data
from .core.db import add_new_rows, db_session, get_matching_rows_from_db
from .core.gdrive import Sheet
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
    Project,
    Sample,
    SequencingRun,
    Tag,
)
from .defaults import (
    CONFIG_DIR,
    DATA_SCHEMAS,
    DB_CONFIG_FILES,
    DB_INIT_FILES,
    DB_SPEC_SCHEMA,
    GDRIVE_CONFIG_FILES,
    GDRIVE_SPEC_SCHEMA,
    SIBLING_REPOSITORY,
    SPLIT_TABLES_JOIN_ON_COLUMNS,
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
    data = {
        path.stem: load_data(path, schema=DATA_SCHEMAS[path.stem])
        for path in data_files.values()
    }

    initial_data = [
        Base.get_model(tablename)(**row)
        for tablename, dataset in data.items()
        for row in dataset
        if tablename != 'lab'
    ]

    Session = db_session(base_class=Base, **spec)
    with Session.begin() as session:
        session.add_all(initial_data)

    with Session.begin() as session:
        cols_to_model_atts = {
            'name': Lab.name,
            'delivery_dir': Lab.delivery_dir,
            'institution.name': Institution.name,
            'pi.first_name': Person.first_name,
            'pi.last_name': Person.last_name,
            'pi.email': Person.email,
            'pi.orcid': Person.orcid,
        }
        add_new_rows(
            session, Lab, cols_to_model_atts, data=data['lab'], data_source='lab.csv'
        )


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
    gdrive_spec: dict = load_data(
        gdrive_config_files['gdrive-spec.yml'],  # schema=GDRIVE_SPEC_SCHEMA
    )

    gclient = gs.service_account(filename=gdrive_config_files['service-account.json'])
    tracking_spreadsheet = gclient.open_by_url(gdrive_spec['spreadsheet_url'])

    main_sheet_id = gdrive_spec['main_sheet_id']
    main_sheet = tracking_spreadsheet.get_worksheet_by_id(main_sheet_id)
    main_sheet_spec = gdrive_spec['worksheets'][main_sheet_id]

    main_data = Sheet(worksheet=main_sheet, **main_sheet_spec).to_records()
    data_source = (
        f'{tracking_spreadsheet.title} - {main_sheet.title} ({main_sheet.url})'
    )

    # TODO: everything below is extremely repetitive. Can it be made into some kind of recursive function?
    Session = db_session(base_class=Base, **db_spec)

    args = [
        (Institution, {'institution.name': Institution.name}),
        (Person, {'pi_name': Person.name}),
        (Person, {'submitter_name': Person.name, 'submitter_email': Person.email}),
    ]
    for argset in args:
        with Session.begin() as session:
            add_new_rows(session, *argset, data=main_data, data_source=data_source)

    # # Add unseen PIs so as to add unseen Labs
    # # Add unseen submitters
    # # Figure out a mechanism to do this without so much repetition (recursive function?)
    # with Session.begin() as session:
    #     person_att_to_sheet_col = {Person.name: 'pi_name'}
    # # TODO: this is so ugly
    # # TODO: figure out a mechanism to capture repeat 3-name or 1-name people. maybe people should be assumed to already be initialized?
    # with Session.begin() as session:
    #     person_att_to_sheet_col = {Person.name: 'submitter_name'}
    #     existent_people = get_matching_rows_from_db(session, person_att_to_sheet_col, data=main_data, data_filename=data_source, raise_error=False)

    #     people_to_add = {(record) for record, existent_person in zip(main_data, existent_people) if existent_person is None}
    #     added_people = []
    #     for record in people_to_add:
    #         first_name, last_name = record['submitter_name'].split(maxsplit=1)
    #         name_parts = record['submitter_name'].split(maxsplit=1)

    #         if len(name_parts) == 2:
    #             first_name, last_name = (name.capitalize() for name in name_parts)
    #             email = record['submitter_email'].lower() if record['submitter_email'] is not None else None

    #             if any(person.first_name == first_name and person.last_name == last_name and person.email == email for person in added_people):
    #                 continue

    #             orcid = Prompt.ask(f'ORCID for [orange1]{first_name} {last_name}[/] ([orange1]{email}[/])', default=None)
    #             person = Person(first_name=first_name, last_name=last_name, email=email, orcid=orcid)
    #         else:
    #             name = record['submitter_name']
    #             library = record['library.id']
    #             new_or_existing = Prompt.ask(f'[orange1]{name}[/] is an invalid submitter. It is associated with library [orange1]{library}[/]. Would you like to enter a new person, or pick an existing person from the database?', choices=['new', 'existing'])
    #             if new_or_existing == 'new':
    #                 first_name = Prompt.ask('First name')
    #                 last_name = Prompt.ask('Last name')
    #                 email = Prompt.ask('Email', default=None)
    #                 orcid = Prompt.ask('ORCID', default=None)
    #                 person = Person(first_name=first_name, last_name=last_name, email=email, orcid=orcid)
    #             else:
    #                 all_people_list = [person for person in existent_people if person is not None] + added_people
    #                 all_people = {f'{person.name} ({person.email})': person for person in all_people_list}
    #                 person_name_email = Prompt.ask('Pick a person', choices=list(all_people.keys()))
    #                 person = all_people[person_name_email]

    #         session.add(person)
    #         added_people.append(person)

    # with Session.begin() as session:
    #     project_att_to_sheet_col = {Project.id: 'project_id'}
    #     existent_projects = get_matching_rows_from_db(session, project_att_to_sheet_col, data=main_data, data_filename=data_source, raise_error=False)

    #     projects_to_add = [record for record, existent_project in zip(main_data, existent_projects) if existent_project is None]

    #     pi_att_to_sheet_col = {Person.name: 'pi_name'}
    #     project_pis = get_matching_rows_from_db(session, pi_att_to_sheet_col, data=projects_to_add, data_filename=data_source, raise_error=False)

    #     submitter_att_to_sheet_col = {Person.name: 'submitter_name', Person.email: 'submitter_email'}
    #     project_submitters = get_matching_rows_from_db(session, submitter_att_to_sheet_col, data=projects_to_add, data_filename=data_source, raise_error=False)

    #     for record, pi, submitter in zip(projects_to_add, project_pis, project_submitters):
    #         project_lab = session.execute(select(Lab).where(Lab.pi==pi)).scalar()

    #         if project_lab is None:
    #             rprint(f'No lab found for PI [orange1]{pi.name}[/]. Please add the lab before adding the project.')
    #             raise typer.Abort()

    #         project = Project(id=record['project_id'], lab=project_lab, people=[pi, submitter])
    #         session.add(project)

    # with Session.begin() as session:
    #     data_set_att_to_sheet_col = {DataSet.date_submitted: 'data_set.date_submitted', DataSet.ilab_request_id: 'data_set.ilab_request_id', DataSet.name: 'data_set.name'}
    #     existent_data_sets = get_matching_rows_from_db(session, data_set_att_to_sheet_col, data=main_data, data_filename=data_source, raise_error=False)

    #     data_sets_to_add = [record for record, existent_data_set in zip(main_data, existent_data_sets) if existent_data_set is None]

    #     project_att_to_sheet_col = {Project.id: 'project_id'}
    #     data_set_projects = get_matching_rows_from_db(session, project_att_to_sheet_col, data=data_sets_to_add, data_filename=data_source, raise_error=False)

    #     platform_att_to_sheet_col = {Platform.name: 'platform.name'}
    #     data_set_platforms = get_matching_rows_from_db(session, platform_att_to_sheet_col, data=data_sets_to_add, data_filename=data_source, raise_error=False)

    #     submitter_att_to_sheet_col = {Person.name: 'submitter_name', Person.email: 'submitter_email'}
    #     data_set_submitters = get_matching_rows_from_db(session, submitter_att_to_sheet_col, data=data_sets_to_add, data_filename=data_source, raise_error=False)

    #     for record, project, platform, submitter in zip(data_sets_to_add, data_set_projects, data_set_platforms, data_set_submitters):
    #         data_set = DataSet(name=record['data_set.name'], date_submitted=record['data_set.date_submitted'], ilab_request_id=record['data_set.ilab_request_id'], project=project, platform=platform, submitter=submitter)
    #         session.add(data_set)

    # with Session.begin() as session:
    #     library_att_to_sheet_col = {Library.id: 'library.id'}
    #     existent_libraries = get_matching_rows_from_db(session, library_att_to_sheet_col, data=main_data, data_filename=data_source, raise_error=False)

    #     libraries_to_add = [record for record, existent_library in zip(main_data, existent_libraries) if existent_library is None]

    #     data_set_att_to_sheet_col = {DataSet.name: 'data_set.name', DataSet.date_submitted: 'data_set.date_submitted', DataSet.ilab_request_id: 'data_set.ilab_request_id'}
    #     library_data_sets = get_matching_rows_from_db(session, data_set_att_to_sheet_col, data=libraries_to_add, data_filename=data_source, raise_error=False)

    #     library_type_att_to_sheet_col = {LibraryType.name: 'library_type.name'}
    #     library_library_types = get_matching_rows_from_db(session, library_type_att_to_sheet_col, data=libraries_to_add, data_filename=data_source, raise_error=False)

    #     sequencing_run_att_to_sheet_col = {SequencingRun.id: 'sequencing_run.id'}
    #     library_sequencing_runs = get_matching_rows_from_db(session, sequencing_run_att_to_sheet_col, data=libraries_to_add, data_filename=data_source, raise_error=False)

    #     for record, data_set, library_type, sequencing_run in zip(libraries_to_add, library_data_sets, library_library_types, library_sequencing_runs):
    #         library_type = record['platform.name']
    #         library = Library(id=record['library.id'], data_set=data_set, library_type=library_type, sequencing_run=sequencing_run)
    #         session.add(library)
