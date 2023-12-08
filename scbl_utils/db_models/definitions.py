"""
This module contains SQLAlchemy models for the `scbl-utils` package.
These models represent the definitions of experimental data stored in
the database, as opposed to the data itself, which is stored in
`data.py`. For example, an `Experment` is really an instance of a
`Platform`.

Classes:
    - `Platform`: Represents an experimental protocol or platform.

    - `LibraryType`: Represents a cDNA library type, such as gene
    expression or chromatin accessibility

    - `Tag`: Represents a tag used to multiplex `Sample`s in a
    `Library`.
"""

from sqlalchemy.orm import Mapped, mapped_column

from .bases import (
    Base,
    int_pk,
    samplesheet_str,
    samplesheet_str_pk,
    stripped_str,
    unique_samplesheet_str,
    unique_stripped_str,
)


class Platform(Base):
    __tablename__ = 'platform'

    id: Mapped[int_pk] = mapped_column(init=False)
    name: Mapped[unique_stripped_str]


class LibraryType(Base):
    __tablename__ = 'library_type'

    id: Mapped[int_pk] = mapped_column(init=False)
    name: Mapped[unique_samplesheet_str]


class Tag(Base):
    __tablename__ = 'tag'

    # TODO: add validation
    id: Mapped[samplesheet_str_pk]
    name: Mapped[samplesheet_str | None]
    tag_type: Mapped[stripped_str]
    read: Mapped[stripped_str]
    sequence: Mapped[stripped_str]
    pattern: Mapped[stripped_str]
    five_prime_offset: Mapped[int]
