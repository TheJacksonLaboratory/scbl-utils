import logging
from collections.abc import Sequence
from functools import cached_property
from itertools import groupby
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import computed_field, field_validator, model_validator
from scbl_db import Base
from scbl_db.bases import Base, Data
from sqlalchemy import inspect, select
from sqlalchemy.orm import RelationshipDirection, RelationshipProperty, Session
from sqlalchemy.util import ReadOnlyProperties

from .db_query import get_model_instance_from_db
from .pydantic_model_config import StrictBaseModel
from .validated_types import _validate_db_target


class DataInserter(
    StrictBaseModel, arbitrary_types_allowed=True, frozen=True, strict=True
):
    data: pl.DataFrame
    model: type[Base]
    session: Session
    source: str | Path
    calling_parent_name: str | None = None

    @field_validator('data', mode='after')
    @classmethod
    def validate_columns(cls, data: pl.DataFrame) -> pl.DataFrame:
        for column in data.columns:
            _validate_db_target(column)

        return data

    @model_validator(mode='after')
    def validate_columns_match_model(self: 'DataInserter') -> 'DataInserter':
        if not all(col.startswith(self.model.__name__) for col in self.data.columns):
            raise ValueError(
                f'All column names in {self.source} must start with {self.model.__name__}'
            )

        return self

    @computed_field
    @cached_property
    def _renamed(self) -> pl.DataFrame:
        return self.data.rename(
            {
                column: column.removeprefix(f'{self.model.__name__}.')
                for column in self.data.columns
            }
        )

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
            (
                relationship_name,
                list(column_list),
            )
            for relationship_name, column_list in groupby(
                sorted_relationship_columns, key=lambda col: col.split('.')[0]
            )
        ]

    @computed_field
    @cached_property
    def _index_column(self) -> str:
        return f'{self.model.__name__}_index'

    @computed_field
    @cached_property
    def _aggregated(self) -> pl.DataFrame:
        expressions = []
        group_by = [col for col in self._renamed.columns]

        for relationship_name, column_list in self._relationship_to_columns:
            if relationship_name == self.calling_parent_name:
                continue

            if (
                self._relationships[relationship_name].direction
                == RelationshipDirection.MANYTOONE
            ):
                expression = (
                    pl.struct(column_list)
                    .alias(relationship_name)
                    .struct.rename_fields(
                        [
                            col.removeprefix(f'{relationship_name}.')
                            for col in column_list
                        ]
                    )
                    .first()
                )

            else:
                expression = pl.col(column_list)

                for col in column_list:
                    group_by.remove(col)

            expressions.append(expression)

        aggregated = self._renamed.group_by(group_by).agg(*expressions)
        return aggregated.with_row_index(name=self._index_column, offset=1)

    @computed_field
    @cached_property
    def _with_id(self) -> pl.DataFrame:
        if not issubclass(self.model, Data):
            return self._aggregated

        if 'id' in self._aggregated.columns:
            return self._aggregated

        year_indicator_length = 2
        pad_length = (
            self.model.id_length - len(self.model.id_prefix) - year_indicator_length
        )

        id_expression = (
            self.model.id_prefix
            + pl.col(self.model.id_date_col).dt.to_string('%y')
            + pl.col(self._index_column)
            .cast(str)
            .str.pad_start(pad_length, fill_char='0')
        )
        return self._aggregated.with_columns(id_expression.alias('id'))

    @computed_field
    @cached_property
    def _with_parents(self) -> pl.DataFrame:
        with_parents = self._with_id

        for relationship_name, _ in self._relationship_to_columns:
            if relationship_name == self.calling_parent_name:
                continue

            relationship = self._relationships[relationship_name]

            if relationship.direction != RelationshipDirection.MANYTOONE:
                continue

            relationship_model = relationship.mapper.class_

            with_parents = with_parents.with_columns(
                pl.col(relationship_name).map_elements(
                    lambda struct: get_model_instance_from_db(
                        struct, model=relationship_model, session=self.session
                    ),
                    return_dtype=pl.Object,
                )
            )

        return with_parents

    @computed_field
    @cached_property
    def _with_children_as_records(self) -> list[dict[str, Any]]:
        df_as_dicts = self._with_parents.to_dicts()
        df = self._with_parents

        for (
            relationship_name,
            relationship_column_list,
        ) in self._relationship_to_columns:
            relationship = self._relationships[relationship_name]

            if relationship.direction != RelationshipDirection.ONETOMANY:
                continue

            primary_key_columns = [col.name for col in inspect(self.model).primary_key]

            if relationship.back_populates is None:
                raise NotImplementedError(
                    'something about how back populated must be set'
                )

            child_df = df.select(
                pl.col(primary_key_columns).map_alias(
                    lambda c: f'{relationship.back_populates}.{c}'
                ),
                *relationship_column_list,
            ).explode(relationship_column_list)

            relationship_model: type[Base] = relationship.mapper.class_

            child_df = child_df.rename(
                {
                    column: f'{relationship_model.__name__}.{column.removeprefix(relationship_name + ".")}'
                    for column in child_df.columns
                }
            )

            child_records = DataInserter(
                data=child_df,
                model=relationship_model,
                session=self.session,
                source=self.source,
                calling_parent_name=relationship.back_populates,
            )._with_children_as_records

            for row in df_as_dicts:
                row[relationship_name] = []

                for child_row in child_records:
                    is_same_row = {row[pk] for pk in primary_key_columns} == {
                        child_row[f'{relationship.back_populates}.{pk}']
                        for pk in primary_key_columns
                    }

                    if is_same_row:
                        init_data = {
                            key: val
                            for key, val in child_row.items()
                            if key in relationship_model.dc_init_field_names()
                            and val is not None
                        }

                        try:
                            child_model = relationship_model(**init_data)
                        except Exception as e:
                            logger_name = f'{__package__}.{relationship_model.__name__}'
                            child_logger = logging.getLogger(logger_name)

                            child_logger.error(
                                f'Could not instantiate the following data as a child of {self.model.__name__}:\n\n{init_data}\n\n{e}\n\n\n'
                            )

                            continue
                        else:
                            row[relationship_name].append(child_model)

        return df_as_dicts

    @computed_field
    @property
    def _model_instances_in_db(self) -> Sequence[Base]:
        return self.session.execute(select(self.model)).scalars().all()

    def to_db(self) -> None:
        for row in self._with_children_as_records:
            init_data = {
                key: val
                for key, val in row.items()
                if key in self.model.dc_init_field_names() and val is not None
            }

            try:
                new_model_instance = self.model(**init_data)
            except Exception as e:
                logger_name = f'{__package__}.{self.model.__name__}'
                logger = logging.getLogger(logger_name)

                logger.error(
                    f'Could not instantiate the following data:\n\n{init_data}\n\n{e}\n\n\n'
                )

                continue

            if new_model_instance in self._model_instances_in_db:
                continue

            try:
                self.session.merge(new_model_instance)
            except Exception as e:
                logger_name = f'{__package__}.{self.model.__name__}'
                logger = logging.getLogger(logger_name)

                logger.error(
                    f'Could not add the following data to the database:\n\n{init_data}\n\n{e}\n\n\n'
                )

                continue
