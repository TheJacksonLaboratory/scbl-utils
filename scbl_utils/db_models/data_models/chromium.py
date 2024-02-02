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
# TODO: make sure that compare operations are correct
from re import findall, search, sub

from sqlalchemy import ForeignKey, null
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates

from scbl_utils.db_models.metadata_models import DataSet, Sample

from ...core.validation import valid_str
from ...defaults import LIBRARY_ID_PATTERN
from ..base import Base
from ..column_types import (
    int_pk,
    samplesheet_str,
    samplesheet_str_pk,
    stripped_str,
    unique_samplesheet_str,
)


class ChromiumDataSet(DataSet):
    # ChromiumDataSet attributes
    assay: Mapped[samplesheet_str | None] = mapped_column(index=True)

    # Child models
    samples: Mapped[list['ChromiumSample']] = relationship(
        back_populates='data_set', default_factory=list, repr=False, compare=False
    )
    libraries: Mapped[list['Library']] = relationship(
        back_populates='data_set', default_factory=list, repr=False, compare=False
    )

    __mapper_args__ = {
        'polymorphic_identity': 'Chromium',
    }


class Tag(Base):
    __tablename__ = 'tag'

    # TODO: add validation
    # Tag attributes
    id: Mapped[samplesheet_str_pk]
    name: Mapped[samplesheet_str | None]
    type: Mapped[stripped_str]
    read: Mapped[stripped_str]
    sequence: Mapped[stripped_str]
    pattern: Mapped[stripped_str]
    five_prime_offset: Mapped[int]


class ChromiumSample(Sample):
    # Parent foreign keys
    # chromium_data_set_id: Mapped[int | None] = mapped_column(ForeignKey('data_set.id'), init=False, repr=False)
    tag_id: Mapped[str | None] = mapped_column(
        ForeignKey('tag.id'), init=False, repr=False
    )

    # Parent models
    # chromium_data_set: Mapped[ChromiumDataSet] = relationship(back_populates='chromium_samples')
    data_set: Mapped[ChromiumDataSet] = relationship(back_populates='samples')
    tag: Mapped[Tag] = relationship(default=None, repr=False)

    __mapper_args__ = {'polymorphic_identity': 'Chromium'}


class SequencingRun(Base, kw_only=True):
    __tablename__ = 'sequencing_run'

    # SequencingRun attributes
    # TODO: validate that this matches a pattern
    id: Mapped[samplesheet_str_pk]

    # Child models
    libraries: Mapped[list['Library']] = relationship(
        back_populates='sequencing_run', default_factory=list, repr=False, compare=False
    )


class LibraryType(Base, kw_only=True):
    __tablename__ = 'library_type'

    # LibraryType attributes
    id: Mapped[int_pk] = mapped_column(init=False, repr=False)
    name: Mapped[unique_samplesheet_str] = mapped_column(index=True)


class Library(Base, kw_only=True):
    __tablename__ = 'library'

    # Library attributes
    id: Mapped[samplesheet_str_pk]
    # TODO: add some validation so that libraries with a particular
    # status must have a sequencing run
    status: Mapped[stripped_str] = mapped_column(compare=False)

    # Parent foreign keys
    data_set_id: Mapped[int] = mapped_column(
        ForeignKey('data_set.id'), init=False, repr=False
    )
    library_type_id: Mapped[int] = mapped_column(
        ForeignKey('library_type.id'), init=False, repr=False
    )
    sequencing_run_id: Mapped[str | None] = mapped_column(
        ForeignKey('sequencing_run.id'),
        init=False,
        insert_default=null(),
        compare=False,
    )

    # Parent models
    data_set: Mapped[ChromiumDataSet] = relationship(back_populates='libraries')
    library_type: Mapped[LibraryType] = relationship()
    sequencing_run: Mapped[SequencingRun] = relationship(
        back_populates='libraries', default=None, repr=False, compare=False
    )

    @validates('id')
    def check_id(self, key: str, id: str) -> str | None:
        id = id.upper().strip()
        if valid_str(
            string=id,
            pattern=LIBRARY_ID_PATTERN,
            string_name='library ID',
        ):
            return id
