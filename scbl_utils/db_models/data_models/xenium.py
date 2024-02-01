from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates

from ..base import Base
from ..column_types import int_pk, samplesheet_str, samplesheet_str_pk, unique_int
from ..metadata_models import DataSet, Sample


class XeniumRun(Base):
    __tablename__ = 'xenium_run'

    id: Mapped[int_pk] = mapped_column(init=False, repr=False)
    # TODO: eventually, we can get rid of this?
    name: Mapped[samplesheet_str] = mapped_column(index=True, unique=True)

    # Child models
    data_sets: Mapped[list['XeniumDataSet']] = relationship(
        back_populates='xenium_run', default_factory=list
    )


class XeniumDataSet(DataSet):
    # XeniumSlide attributes
    slide_id: Mapped[unique_int | None]
    slide_name: Mapped[samplesheet_str | None]

    # Parent foreign keys
    xenium_run_id: Mapped[int | None] = mapped_column(
        ForeignKey('xenium_run.id'), init=False, repr=False
    )

    # Parent models
    xenium_run: Mapped[XeniumRun] = relationship(back_populates='data_sets')

    __mapper_args__ = {'polymorphic_identity': 'Xenium'}

    # TODO: implement this to check against 10x's database?
    @validates('id')
    def check_id(self, key: str, id: int) -> int:
        return id


class XeniumSample(Sample):
    # XeniumSample attributes
    xenium_id: Mapped[samplesheet_str | None]  # TODO: this should be changed
    __mapper_args__ = {'polymorphic_identity': 'Xenium'}
