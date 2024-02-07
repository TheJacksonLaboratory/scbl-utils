from ...base import Base
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates
from sqlalchemy import ForeignKey
from ...custom_types import int_pk, samplesheet_str, unique_int
from ..data import DataSet, Sample
class XeniumRun(Base):
    __tablename__ = 'xenium_run'

    # XeniumRun attributes
    id: Mapped[int_pk] = mapped_column(init=False, repr=False)
    # TODO: eventually, we can get rid of this?
    name: Mapped[samplesheet_str] = mapped_column(index=True, unique=True)

    # Child models
    data_sets: Mapped[list['XeniumDataSet']] = relationship(
        back_populates='xenium_run', default_factory=list, repr=False, compare=False
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

    # Child models
    samples: Mapped[list['XeniumSample']] = relationship(
        back_populates='data_set', default_factory=list, repr=False
    )

    __mapper_args__ = {'polymorphic_identity': 'Xenium'}

    # TODO: implement this to check against 10x's database?
    @validates('id')
    def check_id(self, key: str, id: int) -> int:
        return id


class XeniumSample(Sample):
    # XeniumSample attributes
    xenium_id: Mapped[samplesheet_str | None]  # TODO: this should be changed

    # Parent models
    data_set: Mapped[XeniumDataSet] = relationship(back_populates='samples')
    __mapper_args__ = {'polymorphic_identity': 'Xenium'}