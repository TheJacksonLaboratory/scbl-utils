from collections.abc import Generator, Iterable, Sequence
from functools import cache, cached_property
from itertools import groupby
from pathlib import Path
from typing import Annotated

from pydantic import AfterValidator, StringConstraints, computed_field, model_validator
from scbl_db import ORDERED_MODELS, Base
from scbl_db.bases import Base
from sqlalchemy import inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapper, Session

from .db_query import get_matching_obj
from .pydantic_model_config import StrictBaseModel


@cache
def validate_model_field(model: type[Base], field: str):
    if field.count('.') == 0:
        if field not in model.field_names():
            raise ValueError(f'{field} not a field of {model.__name__}')

        return

    parent_name, parent_field = field.split('.', maxsplit=1)
    relationships = inspect(model).relationships

    if parent_name not in relationships.keys():
        raise ValueError(f'{parent_name} is not a relationship in {model.__name__}')

    parent_model = relationships[parent_name].mapper.class_
    validate_model_field(parent_model, field=parent_field)


@cache
def validate_db_target(db_target: str) -> str:
    model_name, field = db_target.split('.', maxsplit=1)
    model = ORDERED_MODELS[model_name]

    validate_model_field(model, field=field)

    return db_target


DBTarget = Annotated[
    str,
    StringConstraints(
        pattern=rf'({"|".join(model_name for model_name in ORDERED_MODELS)})\.[\w.]+'
    ),
    AfterValidator(validate_db_target),
]


class DataToInsert(
    StrictBaseModel, arbitrary_types_allowed=True, frozen=True, strict=True
):
    columns: tuple[DBTarget, ...]
    data: Iterable[Iterable]
    model: type[Base]
    session: Session
    source: str | Path

    @model_validator(mode='after')
    def validate_columns(self: 'DataToInsert') -> 'DataToInsert':
        if not all(col.startswith(self.model.__name__) for col in self.columns):
            raise ValueError(
                f'All column names in {self.source} must start with {self.model.__name__}'
            )

        return self

    @computed_field
    @cached_property
    def relative_columns(self: 'DataToInsert') -> tuple[str, ...]:
        return tuple(
            col.removeprefix(f'{self.model.__name__}.') for col in self.columns
        )

    @computed_field
    @cached_property
    def columns_as_set(self) -> set[str]:
        return set(self.relative_columns)

    @computed_field
    @cached_property
    def parent_to_columns(self) -> dict[str, tuple[int, ...]]:
        parent_columns = sorted(
            (
                (i, col)
                for (i, col) in enumerate(self.relative_columns)
                if col.count('.') > 0
                and col.split('.')[0] in self.model.init_field_names()
            ),
            key=lambda idx_col: idx_col[1],
        )

        return {
            parent_name: tuple(i for i, col in column_list)
            for parent_name, column_list in groupby(
                parent_columns, key=lambda idx_col: idx_col[1].split('.')[0]
            )
        }

    @computed_field
    @property
    def cleaned_data(self) -> Generator[tuple, None, None]:
        return (
            tuple(val if val != '' else None for val in value_list)
            for value_list in self.data
        )

    @cache
    def assign_parent(
        self,
        row: tuple,
        parent_column_idxs: tuple[int, ...],
        parent_mapper: Mapper[Base],
        parent_name: str,
    ) -> Sequence[Base]:
        parent_columns = tuple(
            self.relative_columns[i].removeprefix(f'{parent_name}.')
            for i in parent_column_idxs
            if row[i] is not None
        )
        parent_row = tuple(row[i] for i in parent_column_idxs if row[i] is not None)

        return get_matching_obj(
            columns=parent_columns,
            row=parent_row,
            session=self.session,
            model_mapper=parent_mapper,
        )

    @cache
    def row_to_model(self, row: tuple) -> Base | None:
        relationships = inspect(self.model).relationships
        row_parents: dict[str, Base | None] = {}

        for parent_name, parent_column_idxs in self.parent_to_columns.items():
            if parent_name in self.relative_columns:
                continue

            parent_mapper = relationships[parent_name].mapper
            found_parents = self.assign_parent(
                row,
                parent_column_idxs=parent_column_idxs,
                parent_mapper=parent_mapper,
                parent_name=parent_name,
            )

            if len(found_parents) != 1:
                if parent_name in self.model.required_init_field_names():
                    break
                else:
                    row_parents[parent_name] = None

            else:
                row_parents[parent_name] = found_parents[0]

        row_as_dict = {
            col: row[i]
            for i, col in enumerate(self.relative_columns)
            if col in self.model.init_field_names()
        }
        model_initializer = row_as_dict | row_parents

        if model_initializer.keys() < self.model.required_init_field_names():
            return

        return self.model(**model_initializer)

    @computed_field
    @cached_property
    def model_instances_in_db(self) -> Sequence[Base]:
        return self.session.execute(select(self.model)).scalars().all()

    # TODO: add some way to track which rows were not added
    def to_db(self) -> None:
        for row in self.cleaned_data:
            model_instance = self.row_to_model(row)

            if model_instance in self.model_instances_in_db or model_instance is None:
                continue

            self.session.add(model_instance)

            try:
                self.session.flush()
            except IntegrityError:
                self.session.expunge(model_instance)
                continue
