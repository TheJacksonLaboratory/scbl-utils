from datetime import date
from os import getenv
from pathlib import Path
from re import match

from email_validator import validate_email
from requests import get
from rich import print as rprint
from sqlalchemy import ForeignKey, inspect, null
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates
from typer import Abort

from ..core.utils import _get_format_string_vars
from ..core.validation import valid_str, validate_dir
from ..defaults import ORCID_PATTERN, PROJECT_ID_PATTERN
from .base import Base
from .column_types import (
    SamplesheetString,
    StrippedString,
    int_pk,
    samplesheet_str,
    stripped_str,
    stripped_str_pk,
    unique_stripped_str,
)
from .join_tables import project_person_mapping


class Institution(Base, kw_only=True):
    __tablename__ = 'institution'

    id: Mapped[int_pk] = mapped_column(init=False)
    email_format: Mapped[stripped_str] = mapped_column(repr=False)
    name: Mapped[unique_stripped_str] = mapped_column(default=None, index=True)
    short_name: Mapped[stripped_str] = mapped_column(default=None, index=True)
    country: Mapped[str] = mapped_column(StrippedString(length=2), default='US')
    state: Mapped[str | None] = mapped_column(
        StrippedString(length=2), default=None, insert_default=null()
    )
    city: Mapped[stripped_str] = mapped_column(default=None)
    ror_id: Mapped[unique_stripped_str | None] = mapped_column(
        default=None, insert_default=null(), repr=False
    )

    # TODO needs more validation to check that the email format is correct
    @validates('email_format')
    def check_email_format(self, key: str, email_format: str) -> str:
        email_format = email_format.strip().lower()

        variables = _get_format_string_vars(email_format)

        if not variables:
            rprint(
                f'The email format [orange1]{email_format}[/] is invalid, as it contains no variables enclosed in curly braces'
                r'({})'
            )
            raise Abort()

        person_columns = set(inspect(Person).columns.keys())
        non_existent_person_columns = variables - person_columns

        if non_existent_person_columns:
            rprint(
                f'The email format [orange1]{email_format}[/] is invalid because it contains the following variables, which are not members of [green]{person_columns}[/]:',
                f'[orange1]{non_existent_person_columns}[/]',
                sep='\n',
            )
            raise Abort()

        example_values = {var: 'string' for var in variables}
        example_email = email_format.format_map(example_values)

        validate_email(example_email)

        return email_format

    @validates('ror_id')
    def check_ror_id(self, key: str, ror_id: str | None) -> str | None:
        if ror_id is None:
            return ror_id

        ror_id = ror_id.strip()
        base_url = 'https://api.ror.org/organizations'
        url = f'{base_url}/{ror_id}'
        response = get(url)

        if not response.ok:
            rprint(
                f'Institution with ROR ID [green]{ror_id}[/] not found in '
                f'database search of {base_url}.'
            )
            raise Abort()

        data = response.json()

        if self.name is None:
            self.name = data['name']

        acronyms = data['acronyms']
        aliases = data['aliases']
        if self.short_name is None:
            if len(acronyms) > 0:
                self.short_name = acronyms[0]
            elif len(aliases) > 0:
                self.short_name = aliases[0]
            else:
                pass

        self.country = data['country']['country_code']

        addresses = data['addresses']
        if len(addresses) > 0:
            geonames_city_info = addresses[0]['geonames_city']
            self.city = geonames_city_info['city']
            if self.country == 'US':
                state_code = geonames_city_info['geonames_admin1']['code']
                _, self.state = state_code.split('.')
        else:
            rprint(
                f'Could not find city information from ROR for {self.name}. '
                'Please enter manually.'
            )
            Abort()

        return ror_id


class Person(Base, kw_only=True):
    __tablename__ = 'person'

    id: Mapped[int_pk] = mapped_column(init=False, repr=False)
    first_name: Mapped[stripped_str] = mapped_column(repr=False)
    last_name: Mapped[stripped_str] = mapped_column(repr=False)

    institution_id: Mapped[int] = mapped_column(
        ForeignKey('institution.id'), repr=False, init=False, compare=False
    )
    institution: Mapped[Institution] = relationship(repr=False, compare=False)

    name: Mapped[stripped_str] = mapped_column(init=False, default=None, index=True)
    email_auto_generated: Mapped[bool] = mapped_column(
        init=False, default=False, repr=False
    )
    email: Mapped[unique_stripped_str] = mapped_column(default=None, index=True)
    orcid: Mapped[unique_stripped_str | None] = mapped_column(
        default=None, insert_default=null(), repr=False
    )

    projects: Mapped[list['Project']] = relationship(
        back_populates='people',
        default_factory=list,
        secondary=project_person_mapping,
        repr=False,
    )

    @validates('first_name', 'last_name')
    def capitalize_name(self, key: str, name: str) -> str:
        formatted_split = name.strip().title().split()
        noramlized_inner_whitespace = ' '.join(formatted_split)
        return noramlized_inner_whitespace

    @validates('name')
    def set_name(self, key: str, name: None) -> str:
        return f'{self.first_name} {self.last_name}'

    @validates('orcid')
    def check_orcid(self, key: str, orcid: str | None) -> str | None:
        if orcid is None:
            return orcid

        orcid = orcid.strip()
        invalid_message = (
            f'The ORCID [orange1]{orcid}[/] (assigned to '
            f'[orange1]{self.first_name} {self.last_name}[/]) is invalid'
        )

        if (match_obj := match(ORCID_PATTERN, string=orcid)) is None:
            rprint(
                f'{invalid_message} because it does not match the pattern '
                f'[green]{ORCID_PATTERN}[/].'
            )
            raise Abort()

        digit_groups = match_obj.groups()
        formatted_orcid = '-'.join(digit_groups)

        base_url = 'https://pub.orcid.org'
        url = f'{base_url}/{formatted_orcid}'
        headers = {'Accept': 'application/json'}
        response = get(url, headers=headers)

        if not response.ok:
            rprint(
                f'{invalid_message} because it was not found with database search of {base_url}'
            )
            raise Abort()

        return formatted_orcid

    @validates('email')
    def check_email(self, key: str, email: str | None) -> str | None:
        variables = _get_format_string_vars(self.institution.email_format)
        var_values = {var: getattr(self, var) for var in variables}

        theoretical_email = (
            self.institution.email_format.format_map(var_values)
            .lower()
            .replace(' ', '')
        )

        email = email.strip().lower() if email is not None else email

        if email is None:
            email = theoretical_email
            rprint(
                f'[yellow bold]WARNING[/]: [orange1]{self.first_name} {self.last_name}[/] has no email. Using [orange1]{email}[/] based on format [orange1]{self.institution.email_format}[/].'
            )
            self.email_auto_generated = True

        elif email != theoretical_email:
            rprint(
                f'[yellow bold]WARNING[/]: [orange1]{self.first_name} {self.last_name}[/]\'s email [orange1]{email.lower()}[/] does not match the email format [orange1]{self.institution.email_format}[/].'
            )

        email_info = validate_email(email, check_deliverability=True)
        return email_info.normalized


class Lab(Base, kw_only=True):
    __tablename__ = 'lab'

    id: Mapped[int_pk] = mapped_column(init=False, repr=False)

    institution_id: Mapped[int] = mapped_column(
        ForeignKey('institution.id'), init=False, repr=False
    )
    pi_id: Mapped[int] = mapped_column(ForeignKey('person.id'), init=False, repr=False)

    institution: Mapped[Institution] = relationship()
    pi: Mapped[Person] = relationship()
    projects: Mapped[list['Project']] = relationship(
        back_populates='lab', default_factory=list, repr=False
    )

    name: Mapped[unique_stripped_str] = mapped_column(default=None, index=True)
    delivery_dir: Mapped[unique_stripped_str] = mapped_column(default=None, repr=False)
    group: Mapped[stripped_str] = mapped_column(init=False, default=None, repr=False)

    @validates('name')
    def set_name(self, key: str, name: str | None) -> str:
        # This assumes that no two PIs share the same name. Might have
        # to change in production
        return (
            f'{self.pi.first_name} {self.pi.last_name} Lab'
            if name is None
            else name.title()
        )

    @validates('delivery_dir')
    def set_delivery_dir(self, key: str, delivery_dir: str | None) -> str:
        # Getting delivery parent dir from environment for the sake of
        # testing. Hopefully can figure out a better way, importing from
        # defaults instead of this
        delivery_parent_str = getenv('DELIVERY_PARENT_DIR', '/sc/service/delivery')
        delivery_parent_dir = Path(delivery_parent_str)

        # If no delivery directory is provided, get it automatically
        # from PI name
        if delivery_dir is None:
            pi = self.pi

            first_name = SamplesheetString().process_bind_param(
                pi.first_name.lower(), dialect=''
            )
            last_name = SamplesheetString().process_bind_param(
                pi.last_name.lower(), dialect=''
            )

            delivery_path = Path(f'{first_name}_{last_name}')
            error_prefix = (
                '[green]scbl-utils[/] tried to automatically generate a '
                f'delivery directory for [orange1]{self.name}[/] using '
                f'the name of the PI [orange1]{self.pi.first_name} {self.pi.last_name}[/].'
            )
        else:
            delivery_path = Path(delivery_dir)
            error_prefix = None

        # Validate and get absolute path
        valid_delivery_paths = validate_dir(
            delivery_parent_dir,
            required_files=[delivery_path],
            create=False,
            error_prefix=error_prefix,
        )
        abs_delivery_path = valid_delivery_paths[delivery_path.name]

        return str(abs_delivery_path)

    @validates('group')
    def set_group(self, key: str, group: None) -> str:  # type: ignore
        return Path(self.delivery_dir).group()


class Project(Base, kw_only=True):
    __tablename__ = 'project'

    # Project attributes
    id: Mapped[stripped_str_pk]
    description: Mapped[stripped_str | None] = mapped_column(default=None, index=True)

    # Parent foreign keys
    lab_id: Mapped[int] = mapped_column(ForeignKey('lab.id'), init=False, repr=False)

    # Parent models
    lab: Mapped[Lab] = relationship(back_populates='projects')

    # Child models
    data_sets: Mapped[list['DataSet']] = relationship(
        back_populates='project', default_factory=list, repr=False
    )
    people: Mapped[list['Person']] = relationship(
        back_populates='projects',
        default_factory=list,
        secondary=project_person_mapping,
        repr=False,
    )

    @validates('id')
    def check_id(self, key: str, id: str) -> str | None:
        checked_id = valid_str(
            string=id.upper().strip(),
            pattern=PROJECT_ID_PATTERN,
            string_name='project ID',
        )
        return checked_id if isinstance(checked_id, str) else None


# TODO: implement some kind of test that makes sure each platform type
# has a corresponding file in the data_models folder
class Platform(Base, kw_only=True):
    __tablename__ = 'platform'

    # Platform attributes
    name: Mapped[stripped_str_pk]


class DataSet(Base, kw_only=True):
    __tablename__ = 'data_set'

    # TODO: add validation/auto-setting for id depending on
    # platform/assay type

    # DataSet attributes
    id: Mapped[int_pk] = mapped_column(repr=False)
    name: Mapped[samplesheet_str] = mapped_column(index=True)
    ilab_request_id: Mapped[stripped_str] = mapped_column(
        index=True
    )  # TODO: ilab request ID validation
    processing_start_date: Mapped[date | None] = mapped_column(default=None, repr=False)

    # Parent foreign keys
    project_id: Mapped[str] = mapped_column(
        ForeignKey('project.id'), init=False, repr=False
    )
    platform_name: Mapped[str] = mapped_column(ForeignKey('platform.name'), init=False)
    submitter_id: Mapped[int] = mapped_column(
        ForeignKey('person.id'), init=False, repr=False
    )

    # Parent models
    platform: Mapped[Platform] = relationship(repr=False)
    project: Mapped[Project] = relationship(back_populates='data_sets')
    submitter: Mapped[Person] = relationship()

    # Child models
    samples: Mapped[list['Sample']] = relationship(
        back_populates='data_set', default_factory=list, repr=False
    )

    # TODO should there be another column for the date that work was
    # begun on the dataset? this will help generate a batch_id

    # Automatically set attributes
    batch_id: Mapped[int] = mapped_column(init=False, default=None, repr=False)

    __mapper_args__ = {
        'polymorphic_on': 'platform_name',
    }

    @validates('batch_id')
    def set_batch_id(self, key: str, batch_id: None) -> int:
        # If it's decided that more things constitute a batch, this will
        # be easy to update.

        # Note that submitter email, institution, name, and ORCID have
        # been picked instead of the person ID because a person is not
        # assigned an ID until they enter the database.

        # There is a small likelihood that two people with the same name
        # and institution both have an autogenerated email and no ORCID.
        # If these two people submit on the same day, the batch_id of
        # the two datasets will be the same.
        # TODO: this should be fixed somehow
        to_hash = (
            self.processing_start_date,
            self.submitter.email,
            self.submitter.institution_id,
            self.submitter.first_name,
            self.submitter.last_name,
            self.submitter.orcid,
        )
        return hash(to_hash)


class Sample(Base, kw_only=True):
    __tablename__ = 'sample'

    # Sample attributes
    id: Mapped[int_pk] = mapped_column(init=False)
    name: Mapped[samplesheet_str] = mapped_column(index=True)
    date_received: Mapped[date] = mapped_column(default_factory=date.today)

    # Parent foreign keys
    data_set_id: Mapped[int] = mapped_column(
        ForeignKey('data_set.id'), init=False, repr=False
    )
    platform_name: Mapped[str] = mapped_column(ForeignKey('platform.name'), init=False)

    # Parent models
    data_set: Mapped[DataSet] = relationship(back_populates='samples')
    platform: Mapped[Platform] = relationship(repr=False)

    __mapper_args__ = {'polymorphic_on': 'platform_name'}
