from sqlalchemy import ForeignKey, null
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates
from ...custom_types import samplesheet_str, samplesheet_str_pk, int_pk, unique_samplesheet_str, stripped_str
from ...base import Base
from ..data import DataSet, Sample
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
    tag_id: Mapped[str | None] = mapped_column(
        ForeignKey('tag.id'), init=False, repr=False
    )

    # Parent models
    data_set: Mapped[ChromiumDataSet] = relationship(back_populates='samples')
    tag: Mapped[Tag] = relationship(default=None, repr=False)

    __mapper_args__ = {'polymorphic_identity': 'Chromium'}


class SequencingRun(Base, kw_only=True):
    __tablename__ = 'sequencing_run'

    # SequencingRun attributes
    # TODO: validate that this matches a pattern
    id: Mapped[samplesheet_str_pk]


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
    sequencing_run: Mapped[SequencingRun] = relationship(default=None, repr=False, compare=False)
    # TODO: add validation
    @validates('id')
    def check_id(self, key: str, id: str) -> str | None:
        return id.upper().strip() if isinstance(id, str) else None
