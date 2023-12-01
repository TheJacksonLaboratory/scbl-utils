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
    
    - `Project`: SCBL project, used to group `Experiment`s. Not to be
    confused with a consortium/project headed by a PI.
    
    - `Person`: A person, who can be on multiple `Project`s.
    
    - `Experiment`: Experiment in a `Project`. This table essentially
    handles the complex mappings between `Sample`s, `Library`s, and
    `Project`s.
    
    - `Sample`: Biological sample in an `Experiment`. Can be associated
    with multiple `Library`s, or multiple `Library`s can be associated
    with it.
    
    - `SequencingRun`: A sequencing run, which can be associated with
    one or more `Library`s.
    
    - `Library`: A cDNA library, the ultimate item that is sequenced.
"""
# TODO: submit entries to ROR for JAX?
# TODO Write docstrings and comments for all classes and methods
from pathlib import Path
from re import match

from email_validator import validate_email
from requests import get
from rich import print as rprint
from sqlalchemy import Column, ForeignKey, Table, null
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates
from typer import Abort

from ..defaults import DELIVERY_PARENT_DIR, ORCID_PATTERN
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

    id: Mapped[int_pk] = mapped_column(init=False)
    name: Mapped[unique_stripped_str] = mapped_column(default=None)
    short_name: Mapped[unique_stripped_str | None] = mapped_column(
        default=None, insert_default=null()
    )
    country: Mapped[str] = mapped_column(StrippedString(length=2), default='US')
    state: Mapped[str | None] = mapped_column(
        StrippedString(length=2), default=None, insert_default=null()
    )
    city: Mapped[stripped_str] = mapped_column(default=None)
    ror_id: Mapped[unique_stripped_str | None] = mapped_column(
        default=None, insert_default=null()
    )

    labs: Mapped[list['Lab']] = relationship(
        back_populates='institution', default_factory=list
    )

    @validates('ror_id')
    def check_ror(self, key: str, ror_id: str | None) -> str | None:
        if ror_id is None:
            return ror_id

        base_url = 'https://api.ror.org/organizations'
        url = f'{base_url}/{ror_id}'
        response = get(url)

        if not response.ok:
            rprint(
                (
                    f'Institution with ROR ID [green]{ror_id}[/] not found in '
                    f'database search of {base_url}.'
                )
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
                (
                    f'Could not find city information from ROR for {self.name}. '
                    'Please enter manually.'
                )
            )
            Abort()

        return ror_id


class Lab(Base):
    __tablename__ = 'lab'

    id: Mapped[int_pk] = mapped_column(init=False)

    institution_id: Mapped[int] = mapped_column(
        ForeignKey('institution.id'), init=False
    )
    pi_id: Mapped[int] = mapped_column(ForeignKey('person.id'), init=False)

    institution: Mapped[Institution] = relationship(back_populates='labs')
    pi: Mapped['Person'] = relationship()
    projects: Mapped[list['Project']] = relationship(
        back_populates='lab', default_factory=list
    )

    name: Mapped[stripped_str] = mapped_column(default=None)
    delivery_dir: Mapped[unique_stripped_str] = mapped_column(default=None)
    group: Mapped[stripped_str] = mapped_column(init=False, default=None)

    @validates('name')
    def set_name(self, key: str, name: str | None) -> str:
        return f'{self.pi.last_name} Lab' if name is None else name.title()

    @validates('delivery_dir')
    def set_delivery_dir(self, key: str, delivery_dir: str | None) -> str:
        if delivery_dir is None:
            pi = self.pi

            first_name = pi.first_name.lower()
            last_name = pi.last_name.lower()

            direc = DELIVERY_PARENT_DIR / f'{first_name}_{last_name}'
            abs_dir = direc.resolve(strict=True)
        else:
            abs_dir = Path(delivery_dir).resolve(strict=True)

            if abs_dir.parent != DELIVERY_PARENT_DIR:
                rprint(f'[orange1]{abs_dir}[/] not in {DELIVERY_PARENT_DIR}.')
                raise Abort()

        if not abs_dir.is_dir():
            dir_not_exist_message = (
                f'the directory [orange1]{abs_dir}[/] does not exist.'
            )

            if delivery_dir is None:
                rprint(
                    (
                        '[green]scbl-utils[/] tried to automatically generate a '
                        f'delivery directory for [orange1]{self.name}[/] using '
                        f'the name of the PI [orange1]{self.pi.name}[/], but '
                        f'{dir_not_exist_message}'
                    )
                )
                raise Abort()

            else:
                rprint(dir_not_exist_message.capitalize())
                raise Abort()

        return str(abs_dir)

    @validates('group')
    def set_group(self, key: str, group: None) -> str:  # type: ignore
        return Path(self.delivery_dir).group()


project_people_mapping = Table(
    'project_people_mapping',
    Base.metadata,
    Column('project_id', ForeignKey('project.id'), primary_key=True),
    Column('person_id', ForeignKey('person.id'), primary_key=True),
)


class Project(Base):
    __tablename__ = 'project'

    id: Mapped[stripped_str_pk]

    lab_id: Mapped[int] = mapped_column(ForeignKey('lab.id'), init=False)

    lab: Mapped[Lab] = relationship(back_populates='projects')
    experiments: Mapped[list['Experiment']] = relationship(
        back_populates='project', default_factory=list
    )
    people: Mapped[list['Person']] = relationship(
        back_populates='projects',
        default_factory=list,
        secondary=project_people_mapping,
    )

    description: Mapped[stripped_str | None] = mapped_column(
        default=None, insert_default=null()
    )


class Person(Base):
    __tablename__ = 'person'

    id: Mapped[int_pk] = mapped_column(init=False)  # TODO: will this become orcid ID?
    first_name: Mapped[stripped_str] = mapped_column(
        repr=False
    )  # TODO: maybe these can just be retrieved from orcid
    last_name: Mapped[stripped_str] = mapped_column(repr=False)
    email: Mapped[unique_stripped_str | None] = mapped_column(
        default=None, insert_default=null()
    )
    name: Mapped[stripped_str] = mapped_column(init=False, default=None)
    orcid: Mapped[unique_stripped_str | None] = mapped_column(
        default=None, insert_default=null()
    )

    projects: Mapped[list[Project]] = relationship(
        back_populates='people', default_factory=list, secondary=project_people_mapping
    )

    @validates('email')
    def check_email(self, key: str, email: str | None) -> str | None:
        if email is None:
            return email

        email_info = validate_email(email, check_deliverability=True)
        return email_info.normalized

    @validates('first_name', 'last_name')
    def capitalize_name(self, key: str, name: str) -> str:
        return name.strip().title()

    @validates('name')
    def set_name(self, key: str, name: None) -> str:  # type: ignore
        return f'{self.first_name} {self.last_name}'

    @validates('orcid')
    def check_orcid(self, key: str, orcid: str | None) -> str | None:
        if orcid is None:
            return orcid

        invalid_message = (
            f'The ORCID [orange1]{orcid}[/] (assigned to '
            f'[orange1]{self.name}[/]) is invalid'
        )

        match_obj = match(ORCID_PATTERN, string=orcid)
        if match_obj is None:
            rprint(
                (
                    f'{invalid_message} because it does not match the pattern '
                    f'[green]{ORCID_PATTERN}[/].'
                )
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
                invalid_message
                + (
                    f'{invalid_message} because it was not found with database '
                    f'search of {base_url}.'
                )
            )
            raise Abort()

        return formatted_orcid


class Experiment(Base):
    __tablename__ = 'experiment'

    id: Mapped[int_pk] = mapped_column(init=False)
    name: Mapped[samplesheet_str]

    project_id: Mapped[str] = mapped_column(ForeignKey('project.id'), init=False)
    platform_id: Mapped[int] = mapped_column(ForeignKey('platform.id'), init=False)
    # TODO: add actual data (species n stuff)

    platform: Mapped[Platform] = relationship()
    project: Mapped[Project] = relationship(back_populates='experiments')

    samples: Mapped[list['Sample']] = relationship(
        back_populates='experiment', default_factory=list
    )
    libraries: Mapped[list['Library']] = relationship(
        back_populates='experiment', default_factory=list
    )


class Sample(Base):
    __tablename__ = 'sample'

    id: Mapped[int_pk] = mapped_column(init=False)
    name: Mapped[samplesheet_str]

    experiment_id: Mapped[int] = mapped_column(ForeignKey('experiment.id'), init=False)
    tag_id: Mapped[str | None] = mapped_column(
        ForeignKey('tag.id'), init=False, insert_default=null()
    )
    # TODO: add actual data

    experiment: Mapped[Experiment] = relationship(back_populates='samples')
    tag: Mapped[Tag] = relationship(default=None)


class SequencingRun(Base):
    __tablename__ = 'sequencing_run'

    id: Mapped[samplesheet_str_pk]

    libraries: Mapped[list['Library']] = relationship(
        back_populates='sequencing_run', default_factory=list
    )


class Library(Base):
    __tablename__ = 'library'

    id: Mapped[samplesheet_str_pk]
    experiment_id: Mapped[int] = mapped_column(ForeignKey('experiment.id'), init=False)
    library_type_id: Mapped[int] = mapped_column(
        ForeignKey('library_type.id'), init=False
    )
    sequencing_run_id: Mapped[str | None] = mapped_column(
        ForeignKey('sequencing_run.id'), init=False, insert_default=null()
    )
    # TODO: add actual data

    experiment: Mapped[Experiment] = relationship(back_populates='libraries')
    library_type: Mapped[LibraryType] = relationship()
    sequencing_run: Mapped[SequencingRun] = relationship(
        back_populates='libraries', default=None
    )
