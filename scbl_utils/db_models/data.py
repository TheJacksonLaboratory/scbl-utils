"""
This module contains SQLAlchemy models for the `scbl-utils` package.
These models represent actual data stored in the database, as opposed to
the definition of the data, which is stored in `definitions.py`.
For example, an `Experment` is really an instance of a `Platform`.

Classes:
    - `Institution`: Research institution, such as a university or
    organization
    
    - `Lab`: Lab at an `Institution`. Can be a PI's lab, or a
    consortium/project headed by a PI.
    
    - `Project`: SCBL project, used to group `data_set`s. Not to be
    confused with a consortium/project headed by a PI.
    
    - `Person`: A person, who can be on multiple `Project`s.
    
    - `DataSet`: data_set in a `Project`. This table essentially
    handles the complex mappings between `Sample`s, `Library`s, and
    `Project`s.
    
    - `Sample`: Biological sample in an `data_set`. Can be associated
    with multiple `Library`s, or multiple `Library`s can be associated
    with it.
    
    - `SequencingRun`: A sequencing run, which can be associated with
    one or more `Library`s.
    
    - `Library`: A cDNA library, the ultimate item that is sequenced.
"""
# TODO: submit entries to ROR for JAX?
from datetime import date
from os import getenv
from pathlib import Path
from re import match

from email_validator import validate_email
from requests import get
from rich import print as rprint
from sqlalchemy import Column, ForeignKey, Table, null
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates
from typer import Abort

from ..core.validation import validate_dir, validate_str
from ..defaults import LIBRARY_ID_PATTERN, ORCID_PATTERN, PROJECT_ID_PATTERN
from .bases import (
    Base,
    StrippedString,
    int_pk,
    samplesheet_str,
    samplesheet_str_pk,
    stripped_str,
    stripped_str_pk,
    unique_stripped_str,
)
from .definitions import LibraryType, Platform, Tag


class Institution(Base):
    __tablename__ = 'institution'

    id: Mapped[int_pk] = mapped_column(init=False, repr=False)
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

    labs: Mapped[list['Lab']] = relationship(
        back_populates='institution', default_factory=list, repr=False
    )

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


class Lab(Base):
    __tablename__ = 'lab'

    id: Mapped[int_pk] = mapped_column(init=False, repr=False)

    institution_id: Mapped[int] = mapped_column(
        ForeignKey('institution.id'), init=False, repr=False
    )
    pi_id: Mapped[int] = mapped_column(ForeignKey('person.id'), init=False, repr=False)

    institution: Mapped[Institution] = relationship(back_populates='labs')
    pi: Mapped['Person'] = relationship()
    projects: Mapped[list['Project']] = relationship(
        back_populates='lab', default_factory=list, repr=False
    )

    name: Mapped[unique_stripped_str] = mapped_column(default=None, index=True)
    delivery_dir: Mapped[unique_stripped_str] = mapped_column(default=None, repr=False)
    group: Mapped[stripped_str] = mapped_column(init=False, default=None, repr=False)

    @validates('name')
    def set_name(self, key: str, name: str | None) -> str:
        # This assumes that no two PIs share the same name and
        # institution. Might have to change in production
        return (
            f'{self.pi.name} Lab - {self.institution.short_name}'
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

            first_name = pi.first_name.lower()
            last_name = pi.last_name.lower()

            delivery_path = Path(f'{first_name}_{last_name}')
            error_prefix = (
                '[green]scbl-utils[/] tried to automatically generate a '
                f'delivery directory for [orange1]{self.name}[/] using '
                f'the name of the PI [orange1]{self.pi.name}[/].'
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


project_person_mapping = Table(
    'project_person_mapping',
    Base.metadata,
    Column('project_id', ForeignKey('project.id'), primary_key=True),
    Column('person_id', ForeignKey('person.id'), primary_key=True),
)


class Project(Base):
    __tablename__ = 'project'

    id: Mapped[stripped_str_pk] = mapped_column()

    lab_id: Mapped[int] = mapped_column(ForeignKey('lab.id'), init=False, repr=False)

    lab: Mapped[Lab] = relationship(back_populates='projects')
    data_sets: Mapped[list['DataSet']] = relationship(
        back_populates='project', default_factory=list, repr=False
    )
    people: Mapped[list['Person']] = relationship(
        back_populates='projects',
        default_factory=list,
        secondary=project_person_mapping,
        repr=False,
    )

    description: Mapped[stripped_str | None] = mapped_column(
        default=None, insert_default=null(), index=True
    )

    @validates('id')
    def check_id(self, key: str, id: str) -> str:
        return validate_str(
            string=id.upper().strip(),
            pattern=PROJECT_ID_PATTERN,
            string_name='project ID',
        )


class Person(Base):
    __tablename__ = 'person'

    id: Mapped[int_pk] = mapped_column(init=False, repr=False)
    first_name: Mapped[stripped_str] = mapped_column(repr=False)
    last_name: Mapped[stripped_str] = mapped_column(repr=False)
    email: Mapped[unique_stripped_str | None] = mapped_column(
        default=None, insert_default=null(), index=True
    )
    name: Mapped[stripped_str] = mapped_column(init=False, default=None, index=True)
    orcid: Mapped[unique_stripped_str | None] = mapped_column(
        default=None, insert_default=null(), repr=False
    )

    projects: Mapped[list[Project]] = relationship(
        back_populates='people',
        default_factory=list,
        secondary=project_person_mapping,
        repr=False,
    )

    @validates('email')
    def check_email(self, key: str, email: str | None) -> str | None:
        if email is None:
            return email

        email_info = validate_email(email.lower(), check_deliverability=True)
        return email_info.normalized

    @validates('first_name', 'last_name')
    def capitalize_name(self, key: str, name: str) -> str:
        formatted = name.strip().title()
        noramlized_inner_whitespace = ' '.join(formatted.split())
        return noramlized_inner_whitespace

    @validates('name')
    def set_name(self, key: str, name: None) -> str:  # type: ignore
        return f'{self.first_name} {self.last_name}'

    @validates('orcid')
    def check_orcid(self, key: str, orcid: str | None) -> str | None:
        if orcid is None:
            return orcid

        orcid = orcid.strip()
        invalid_message = (
            f'The ORCID [orange1]{orcid}[/] (assigned to '
            f'[orange1]{self.name}[/]) is invalid'
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


class DataSet(Base):
    __tablename__ = 'data_set'

    id: Mapped[int_pk] = mapped_column(init=False, repr=False)
    name: Mapped[samplesheet_str] = mapped_column(index=True)
    ilab_request_id: Mapped[stripped_str] = mapped_column(
        index=True
    )  # TODO: ilab validation

    project_id: Mapped[str] = mapped_column(
        ForeignKey('project.id'), init=False, repr=False
    )
    platform_id: Mapped[int] = mapped_column(
        ForeignKey('platform.id'), init=False, repr=False
    )
    submitter_id: Mapped[int] = mapped_column(
        ForeignKey('person.id'), init=False, repr=False
    )
    # TODO: add actual data (species n stuff)

    platform: Mapped[Platform] = relationship()
    project: Mapped[Project] = relationship(back_populates='data_sets')
    submitter: Mapped[Person] = relationship()

    # TODO: there should be another column for the date that work was begun on the dataset (?)
    date_submitted: Mapped[date] = mapped_column(default_factory=date.today)
    batch_id: Mapped[int] = mapped_column(init=False, default=None, repr=False)

    samples: Mapped[list['Sample']] = relationship(
        back_populates='data_set', default_factory=list, repr=False
    )
    libraries: Mapped[list['Library']] = relationship(
        back_populates='data_set', default_factory=list, repr=False
    )

    @validates('batch_id')
    def set_batch_id(self, key: str, batch_id: None) -> int:
        # If it's decided that more things constitute a batch, this will
        # be easy to update.

        # Note that submitter name and email have been picked instead of
        # the person ID because a person is not assigned an ID until
        # they enter the database.

        # Note also that two people with the same name might submit on
        # the same day, so their emails have been included in the hash
        # as well.

        # However, not everyone has an email because the field is not
        # required because it might be unreasonable to expect a wet-lab
        # person to track down an obscure email before being able to
        # enter a person into the database.

        # Therefore, we are relying on the low probability that two
        # email-less people with the same name will submit samples on
        # the same day.
        to_hash = (self.date_submitted, self.submitter.name, self.submitter.email)
        return hash(to_hash)


class Sample(Base):
    __tablename__ = 'sample'

    id: Mapped[int_pk] = mapped_column(init=False, repr=False)
    name: Mapped[samplesheet_str] = mapped_column(index=True)

    data_set_id: Mapped[int] = mapped_column(
        ForeignKey('data_set.id'), init=False, repr=False
    )
    tag_id: Mapped[str | None] = mapped_column(
        ForeignKey('tag.id'), init=False, insert_default=null()
    )
    # TODO: add actual data

    data_set: Mapped[DataSet] = relationship(back_populates='samples')
    tag: Mapped[Tag] = relationship(default=None, repr=False)


class SequencingRun(Base):
    __tablename__ = 'sequencing_run'

    # TODO: validate that this matches a pattern
    id: Mapped[samplesheet_str_pk]

    libraries: Mapped[list['Library']] = relationship(
        back_populates='sequencing_run', default_factory=list, repr=False
    )


class Library(Base):
    __tablename__ = 'library'

    id: Mapped[samplesheet_str_pk]
    data_set_id: Mapped[int] = mapped_column(
        ForeignKey('data_set.id'), init=False, repr=False
    )
    library_type_id: Mapped[int] = mapped_column(
        ForeignKey('library_type.id'), init=False, repr=False
    )
    sequencing_run_id: Mapped[str | None] = mapped_column(
        ForeignKey('sequencing_run.id'), init=False, insert_default=null()
    )
    # TODO: add actual data

    data_set: Mapped[DataSet] = relationship(back_populates='libraries')
    library_type: Mapped[LibraryType] = relationship()
    sequencing_run: Mapped[SequencingRun] = relationship(
        back_populates='libraries', default=None, repr=False
    )

    @validates('id')
    def check_id(self, key: str, id: str) -> str:
        return validate_str(
            string=id.upper().strip(),
            pattern=LIBRARY_ID_PATTERN,
            string_name='library ID',
        )
