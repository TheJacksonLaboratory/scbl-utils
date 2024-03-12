from collections.abc import Generator, Iterable, Sequence
from functools import cache, cached_property
from itertools import groupby
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import computed_field, field_validator, model_validator
from scbl_db import Base
from scbl_db.bases import Base
from sqlalchemy import inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapper, RelationshipDirection, RelationshipProperty, Session
from sqlalchemy.util import ReadOnlyProperties

from .db_query import construct_or_retrieve_model_instance, get_matching_obj
from .pydantic_model_config import StrictBaseModel
from .validated_types import DBTarget, _validate_db_target


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
    def _relative_columns(self: 'DataToInsert') -> tuple[str, ...]:
        return tuple(
            col.removeprefix(f'{self.model.__name__}.') for col in self.columns
        )

    @computed_field
    @cached_property
    def _columns_as_set(self) -> set[str]:
        return set(self._relative_columns)

    @computed_field
    @cached_property
    def _parent_to_columns(self) -> dict[str, tuple[int, ...]]:
        parent_columns = sorted(
            (
                (i, col)
                for (i, col) in enumerate(self._relative_columns)
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
    def _cleaned_data(self) -> Generator[tuple, None, None]:
        return (
            tuple(val if val != '' else None for val in value_list)
            for value_list in self.data
        )

    @cache
    def _assign_parent(
        self,
        row: tuple,
        parent_column_idxs: tuple[int, ...],
        parent_mapper: Mapper[Base],
        parent_name: str,
    ) -> Sequence[Base]:
        parent_columns = tuple(
            self._relative_columns[i].removeprefix(f'{parent_name}.')
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
    def _row_to_model(self, row: tuple) -> Base | None:
        relationships = inspect(self.model).relationships
        row_parents: dict[str, Base | None] = {}

        for parent_name, parent_column_idxs in self._parent_to_columns.items():
            if parent_name in self._relative_columns:
                continue

            parent_mapper = relationships[parent_name].mapper
            found_parents = self._assign_parent(
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
            col: val
            for col, val in zip(self._relative_columns, row, strict=True)
            if col in self.model.init_field_names()
        }
        model_initializer = row_as_dict | row_parents

        if model_initializer.keys() < self.model.required_init_field_names():
            return

        return self.model(**model_initializer)

    @computed_field
    @cached_property
    def _model_instances_in_db(self) -> Sequence[Base]:
        return self.session.execute(select(self.model)).scalars().all()

    # TODO: add some way to track which rows were not added
    def to_db(self) -> None:
        for row in self._cleaned_data:
            model_instance = self._row_to_model(row)

            if model_instance in self._model_instances_in_db or model_instance is None:
                continue

            self.session.add(model_instance)

            try:
                self.session.flush()
            except IntegrityError:
                self.session.expunge(model_instance)
                continue


class DataToInsert2(
    StrictBaseModel, arbitrary_types_allowed=True, frozen=True, strict=True
):
    data: pl.DataFrame | pl.LazyFrame
    model: type[Base]
    session: Session
    source: str | Path

    @field_validator('data', mode='after')
    @classmethod
    def validate_columns(
        cls, data: pl.DataFrame | pl.LazyFrame
    ) -> pl.DataFrame | pl.LazyFrame:
        for column in data.columns:
            _validate_db_target(column)

        return data

    @model_validator(mode='after')
    def validate_columns_match_model(self: 'DataToInsert2') -> 'DataToInsert2':
        if not all(col.startswith(self.model.__name__) for col in self.data.columns):
            raise ValueError(
                f'All column names in {self.source} must start with {self.model.__name__}'
            )

        return self

    @computed_field
    @cached_property
    def _renamed(self) -> pl.DataFrame | pl.LazyFrame:
        return self.data.rename(
            {
                column: column.removeprefix(f'{self.model.__name__}.')
                for column in self.data.columns
            }
        )

    @computed_field
    @cached_property
    def _self_columns(self) -> list[str]:
        return [column for column in self._renamed.columns if column.count('.') == 0]

    @computed_field
    @cached_property
    def _relationships(self) -> ReadOnlyProperties[RelationshipProperty[Any]]:
        return inspect(self.model).relationships

    @computed_field
    @cached_property
    def _relationship_to_columns(self) -> list[tuple[str, list[str]]]:
        sorted_relationship_columns = sorted(
            col for col in self._renamed.columns if col.count('.') > 0
        )

        return [
            (relationship_name, list(column_list))
            for relationship_name, column_list in groupby(
                sorted_relationship_columns, key=lambda col: col.split('.')[0]
            )
        ]

    @computed_field
    @cached_property
    def _parent_to_columns(self) -> list[tuple[str, list[str]]]:
        return [
            (relationship_name, column_list)
            for relationship_name, column_list in self._relationship_to_columns
            if self._relationships[relationship_name].direction
            == RelationshipDirection.MANYTOONE
        ]

    @computed_field
    @cached_property
    def _aggregated(self) -> pl.DataFrame | pl.LazyFrame:
        expressions = []

        for parent_name, column_list in self._parent_to_columns:
            parent_model = self._relationships[parent_name].mapper.class_
            renamed_columns = pl.col(column_list).name.map(
                lambda col: col.replace(f'{parent_name}.', f'{parent_model.__name__}.')
            )

            expressions.append(pl.struct(renamed_columns).alias(parent_name))

        return self._renamed.group_by(self._self_columns).agg(*expressions)

    @computed_field
    @cached_property
    def _with_parents(self) -> pl.DataFrame | pl.LazyFrame:
        df = self._aggregated

        for parent_name, _ in self._parent_to_columns:
            parent_model = self._relationships[parent_name].mapper.class_
            df = df.with_columns(
                pl.col(parent_name).map_elements(
                    function=lambda data: construct_or_retrieve_model_instance(
                        data, session=self.session, model=parent_model
                    )
                )
            )

        return df

    def to_db(self):
        df = self._with_parents

        df_as_structs = df.with_columns(pl.struct(pl.all()).alias(self.model.__name__))
        df_as_models = df_as_structs.select(
            pl.col(self.model.__name__).map_elements(
                function=lambda data: construct_or_retrieve_model_instance(
                    data, session=self.session, model=self.model
                )
            )
        )

        df_as_models = (
            df_as_models.collect()
            if isinstance(df_as_models, pl.LazyFrame)
            else df_as_models
        )

        for model_instance in df_as_models.get_column(self.model.__name__):
            self.session.add(model_instance)

            try:
                self.session.flush()
            except IntegrityError:
                self.session.expunge(model_instance)
                continue


# TODO: next steps
# constuct parent dataframe as a groupby of child dataframes, aggregating the children's attributes
# rename columns in this new dataframe
# create a list of lists of children from these child attributes for each parent (example: dataset libraries samples)
