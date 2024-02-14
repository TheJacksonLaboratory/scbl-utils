from os import environ
from pathlib import Path
from re import match

from email_validator import validate_email
from requests import get
from rich import print as rich_print
from sqlalchemy import ForeignKey, UniqueConstraint, inspect
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates

from ....data_io.validators import validate_directory
from ...helpers import get_format_string_vars
from ..base import Base
from ..custom_types import (
    SamplesheetString,
    StrippedString,
    int_pk,
    stripped_str,
    unique_stripped_str,
)


class Institution(Base, kw_only=True):
    __tablename__ = 'institution'

    id: Mapped[int_pk] = mapped_column(init=False, repr=False, compare=False)
    email_format: Mapped[stripped_str] = mapped_column(repr=False, compare=False)
    name: Mapped[unique_stripped_str] = mapped_column(default=None, index=True)
    short_name: Mapped[stripped_str] = mapped_column(
        default=None, index=True, compare=False
    )
    country: Mapped[str] = mapped_column(StrippedString(length=2), default='US')
    state: Mapped[str | None] = mapped_column(StrippedString(length=2), default=None)
    city: Mapped[stripped_str] = mapped_column(default=None)
    ror_id: Mapped[unique_stripped_str | None] = mapped_column(
        default=None, repr=False, compare=False
    )

    @validates('email_format')
    def check_email_format(self, key: str, email_format: str) -> str:
        email_format = email_format.strip().lower()

        variables = get_format_string_vars(email_format)

        if not variables:
            raise ValueError(f'No variables found in email format {email_format}')

        person_columns = set(inspect(Person).columns.keys())
        non_existent_person_columns = variables - person_columns

        if non_existent_person_columns:
            raise ValueError(
                f'The following variables in {email_format} are not members of {person_columns}:\n{non_existent_person_columns}'
            )

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
            raise ValueError(
                f'ROR ID {ror_id} not found in database search of {base_url}'
            )

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
            raise ValueError(
                f'Could not find city information from ROR for {self.name}. Please enter manually.'
            )

        return ror_id


class Person(Base, kw_only=True):
    __tablename__ = 'person'

    id: Mapped[int_pk] = mapped_column(init=False, repr=False, compare=False)
    first_name: Mapped[stripped_str]
    last_name: Mapped[stripped_str]
    # name: Mapped[stripped_str] = mapped_column(init=False, default=None, index=True)

    institution_id: Mapped[int] = mapped_column(
        ForeignKey('institution.id'), repr=False, init=False, compare=False
    )
    institution: Mapped[Institution] = relationship(repr=False, compare=False)

    email_auto_generated: Mapped[bool] = mapped_column(
        init=False, default=False, repr=False, compare=False
    )
    email: Mapped[unique_stripped_str] = mapped_column(default=None, index=True)
    orcid: Mapped[unique_stripped_str | None] = mapped_column(
        default=None, repr=False, compare=False
    )

    @validates('first_name', 'last_name')
    def format_name(self, key: str, name: str) -> str:
        formatted_split = name.strip().title().split()
        noramlized_inner_whitespace = ' '.join(formatted_split)
        return noramlized_inner_whitespace

    @validates('orcid')
    def check_orcid(self, key: str, orcid: str | None) -> str | None:
        if orcid is None:
            return orcid

        orcid = orcid.strip()

        orcid_pattern = r'^(\d{4})-?(\d{4})-?(\d{4})-?(\d{4}|\d{3}X)$'
        if (match_obj := match(orcid_pattern, string=orcid)) is None:
            raise ValueError(f'ORCID {orcid} does not match pattern {orcid_pattern}')

        digit_groups = match_obj.groups()
        formatted_orcid = '-'.join(digit_groups)

        base_url = 'https://pub.orcid.org'
        url = f'{base_url}/{formatted_orcid}'
        headers = {'Accept': 'application/json'}
        response = get(url, headers=headers)

        if not response.ok:
            raise ValueError(
                f'{formatted_orcid} not found in database search of {base_url}'
            )

        return formatted_orcid

    @validates('email')
    def check_email(self, key: str, email: str | None) -> str | None:
        variables = get_format_string_vars(self.institution.email_format)
        var_values = {var: getattr(self, var) for var in variables}

        theoretical_email = (
            self.institution.email_format.format_map(var_values)
            .lower()
            .replace(' ', '')
        )

        email = email.strip().lower() if email is not None else email

        if email is None:
            email = theoretical_email
            rich_print(
                f'[yellow bold]WARNING[/]: [orange1]{self.first_name} {self.last_name}[/] has no email. Using [orange1]{email}[/] based on format [orange1]{self.institution.email_format}[/].'
            )
            self.email_auto_generated = True

        elif email != theoretical_email:
            rich_print(
                f'[yellow bold]WARNING[/]: [orange1]{self.first_name} {self.last_name}[/]\'s email [orange1]{email.lower()}[/] does not match the email format [orange1]{self.institution.email_format}[/].'
            )

        email_info = validate_email(email, check_deliverability=True)
        return email_info.normalized


class Lab(Base, kw_only=True):
    __tablename__ = 'lab'

    id: Mapped[int_pk] = mapped_column(init=False, repr=False, compare=False)

    institution_id: Mapped[int] = mapped_column(
        ForeignKey('institution.id'), init=False, repr=False, compare=False
    )
    pi_id: Mapped[int] = mapped_column(
        ForeignKey('person.id'), init=False, repr=False, compare=False
    )

    institution: Mapped[Institution] = relationship()
    pi: Mapped[Person] = relationship()

    name: Mapped[stripped_str] = mapped_column(default=None, index=True)
    delivery_dir: Mapped[unique_stripped_str] = mapped_column(default=None, repr=False)
    unix_group: Mapped[stripped_str] = mapped_column(
        init=False, default=None, repr=False, compare=False
    )

    __table_args__ = (UniqueConstraint('institution_id', 'pi_id', 'name'),)

    @validates('name')
    def set_name(self, key: str, name: str | None) -> str:
        # This assumes that no two PIs share the same name and
        # institution. Might have to change in production
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
        delivery_parent_dir = Path(environ['delivery_parent_dir'])

        # If no delivery directory is provided, get it automatically
        # from PI name
        if delivery_dir is None:
            pi = self.pi

            first_name = SamplesheetString().process_bind_param(
                pi.first_name.lower(), dialect=None
            )
            last_name = SamplesheetString().process_bind_param(
                pi.last_name.lower(), dialect=None
            )

            delivery_path = Path(f'{first_name}_{last_name}')
        else:
            delivery_path = Path(delivery_dir)

        validate_directory(delivery_parent_dir, required_structure={delivery_path: []})

        return str(delivery_parent_dir / delivery_path)

    @validates('unix_group')
    def set_group(self, key: str, unix_group: None) -> str:  # type: ignore
        return Path(self.delivery_dir).group()
