from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates

from ...base import Base
from ...custom_types import (
    samplesheet_str,
    samplesheet_str_pk,
    xenium_slide_serial_number,
)
from ..data import DataSet, Sample


class XeniumRun(Base):
    __tablename__ = 'xenium_run'

    # XeniumRun attributes
    id: Mapped[samplesheet_str_pk]

    # Child models
    data_sets: Mapped[list['XeniumDataSet']] = relationship(
        back_populates='xenium_run', default_factory=list, repr=False, compare=False
    )

    # TODO: implement validation
    @validates('id')
    def check_id(self, key: str, id: str) -> str:
        return id.strip().upper()


class XeniumDataSet(DataSet):
    # XeniumDataSet attributes
    slide_serial_number: Mapped[xenium_slide_serial_number | None]
    slide_name: Mapped[samplesheet_str | None]

    # Parent foreign keys
    xenium_run_id: Mapped[int | None] = mapped_column(
        ForeignKey('xenium_run.id'), init=False, repr=False
    )

    # Parent models
    xenium_run: Mapped[XeniumRun] = relationship(back_populates='data_sets')

    # Child models
    samples: Mapped[list['XeniumSample']] = relationship(
        back_populates='data_set', default_factory=list, repr=False
    )

    __mapper_args__ = {'polymorphic_identity': 'Xenium'}

    # TODO: implement this to check against 10x's database?
    # TODO: validate that it's actually an integer with the proper length
    @validates('slide_serial_number')
    def check_slide_serial_number(self, key: str, serial_number: str) -> str:
        return serial_number.strip()


class XeniumSample(Sample):
    # Parent models
    data_set: Mapped[XeniumDataSet] = relationship(back_populates='samples')
    __mapper_args__ = {'polymorphic_identity': 'Xenium'}
