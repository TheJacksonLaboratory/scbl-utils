from collections.abc import Generator, Iterable, Sequence
from datetime import date
from functools import cache, cached_property
from itertools import groupby
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import computed_field, field_validator, model_validator
from requests import session
from scbl_db import Base
from scbl_db.bases import Base, Data
from sqlalchemy import inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapper, RelationshipDirection, RelationshipProperty, Session
from sqlalchemy.util import ReadOnlyProperties

from .db_query import get_model_instance_from_db
from .pydantic_model_config import StrictBaseModel
from .validated_types import DBModelName, DBTarget, _validate_db_target

# class DataToInsert(
#     StrictBaseModel, arbitrary_types_allowed=True, frozen=True, strict=True
# ):
#     columns: tuple[DBTarget, ...]
#     data: Iterable[Iterable]
#     model: type[Base]
#     session: Session
#     source: str | Path

#     @model_validator(mode='after')
#     def validate_columns(self: 'DataToInsert') -> 'DataToInsert':
#         if not all(col.startswith(self.model.__name__) for col in self.columns):
#             raise ValueError(
#                 f'All column names in {self.source} must start with {self.model.__name__}'
#             )

#         return self

#     @computed_field
#     @cached_property
#     def _relative_columns(self: 'DataToInsert') -> tuple[str, ...]:
#         return tuple(
#             col.removeprefix(f'{self.model.__name__}.') for col in self.columns
#         )

#     @computed_field
#     @cached_property
#     def _columns_as_set(self) -> set[str]:
#         return set(self._relative_columns)

#     @computed_field
#     @cached_property
#     def _parent_to_columns(self) -> dict[str, tuple[int, ...]]:
#         parent_columns = sorted(
#             (
#                 (i, col)
#                 for (i, col) in enumerate(self._relative_columns)
#                 if col.count('.') > 0
#                 and col.split('.')[0] in self.model.init_field_names()
#             ),
#             key=lambda idx_col: idx_col[1],
#         )

#         return {
#             parent_name: tuple(i for i, col in column_list)
#             for parent_name, column_list in groupby(
#                 parent_columns, key=lambda idx_col: idx_col[1].split('.')[0]
#             )
#         }

#     @computed_field
#     @property
#     def _cleaned_data(self) -> Generator[tuple, None, None]:
#         return (
#             tuple(val if val != '' else None for val in value_list)
#             for value_list in self.data
#         )

#     @cache
#     def _assign_parent(
#         self,
#         row: tuple,
#         parent_column_idxs: tuple[int, ...],
#         parent_mapper: Mapper[Base],
#         parent_name: str,
#     ) -> Sequence[Base]:
#         parent_columns = tuple(
#             self._relative_columns[i].removeprefix(f'{parent_name}.')
#             for i in parent_column_idxs
#             if row[i] is not None
#         )
#         parent_row = tuple(row[i] for i in parent_column_idxs if row[i] is not None)

#         return get_matching_obj(
#             columns=parent_columns,
#             row=parent_row,
#             session=self.session,
#             model_mapper=parent_mapper,
#         )

#     @cache
#     def _row_to_model(self, row: tuple) -> Base | None:
#         relationships = inspect(self.model).relationships
#         row_parents: dict[str, Base | None] = {}

#         for parent_name, parent_column_idxs in self._parent_to_columns.items():
#             if parent_name in self._relative_columns:
#                 continue

#             parent_mapper = relationships[parent_name].mapper
#             found_parents = self._assign_parent(
#                 row,
#                 parent_column_idxs=parent_column_idxs,
#                 parent_mapper=parent_mapper,
#                 parent_name=parent_name,
#             )

#             if len(found_parents) != 1:
#                 if parent_name in self.model.required_init_field_names():
#                     break
#                 else:
#                     row_parents[parent_name] = None

#             else:
#                 row_parents[parent_name] = found_parents[0]

#         row_as_dict = {
#             col: val
#             for col, val in zip(self._relative_columns, row, strict=True)
#             if col in self.model.init_field_names()
#         }
#         model_initializer = row_as_dict | row_parents

#         if model_initializer.keys() < self.model.required_init_field_names():
#             return

#         return self.model(**model_initializer)

#     @computed_field
#     @cached_property
#     def _model_instances_in_db(self) -> Sequence[Base]:
#         return self.session.execute(select(self.model)).scalars().all()

#     # TODO: add some way to track which rows were not added
#     def to_db(self) -> None:
#         for row in self._cleaned_data:
#             model_instance = self._row_to_model(row)

#             if model_instance in self._model_instances_in_db or model_instance is None:
#                 continue

#             self.session.add(model_instance)

#             try:
#                 self.session.flush()
#             except IntegrityError:
#                 self.session.expunge(model_instance)
#                 continue


class DataToInsert2(
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
    def validate_columns_match_model(self: 'DataToInsert2') -> 'DataToInsert2':
        if not all(col.startswith(self.model.__name__) for col in self.data.columns):
            raise ValueError(
                f'All column names in {self.source} must start with {self.model.__name__}'
            )

        return self

    @computed_field
    @cached_property
    def _skipped_data(self) -> dict[str, list[tuple[dict[str, Any], str]]]:
        return {}

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
    def _self_columns(self) -> list[str]:
        self_columns = [
            column for column in self._renamed.columns if column.count('.') == 0
        ]

        if self.calling_parent_name is not None:
            self_columns.extend(
                (
                    column
                    for column in self._renamed.columns
                    if column.startswith(self.calling_parent_name)
                )
            )

        return self_columns

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

            expressions.append(expression)

        aggregated = self._renamed.group_by(self._self_columns).agg(*expressions)
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
    def _with_children(self) -> pl.DataFrame:
        df = self._with_id

        for relationship_name, column_list in self._relationship_to_columns:
            relationship = self._relationships[relationship_name]
            relationship_model = relationship.mapper.class_
            remote_side = relationship.remote_side

            if not df.schema[relationship_name] == pl.List:
                continue

            primary_key_columns = [col.name for col in inspect(self.model).primary_key]

            child_df = df.select(
                pl.col(primary_key_columns).map_alias(lambda c: f'{remote_side}.{c}'),
                column_list,
            ).explode(column_list)
            child_df = child_df.rename(
                {
                    column: f'{relationship_model}.{column}'
                    for column in child_df.columns
                }
            )

            child_df = DataToInsert2(
                data=child_df,
                model=relationship_model,
                session=self.session,
                source=self.source,
                calling_parent_name=relationship_name,
            ).to_models()

            df = df.join(
                child_df,
                left_on=primary_key_columns,
                right_on=[
                    column
                    for column in child_df.columns
                    if column.startswith(remote_side)
                ],
            )

        return df

    @computed_field
    @cached_property
    def _with_parents(self) -> list[dict[str, Any]]:
        with_relationships = self._with_children_ids.to_dicts()

        for relationship_name, _ in self._relationship_to_columns:
            relationship = self._relationships[relationship_name]
            relationship_model = relationship.mapper.class_

            # TODO: move around the iterations here
            if relationship.direction == RelationshipDirection.MANYTOONE:
                for row in with_relationships:
                    new_row = get_model_instance_from_db(
                        row[relationship_name],
                        model=relationship_model,
                        session=self.session,
                    )
                    if new_row is None:
                        del row[relationship_name]
                    else:
                        row[relationship_name] = new_row

            else:
                for row in with_relationships:
                    new_row = []

                    for child_struct in row[relationship_name]:
                        init_data = {
                            key: val
                            for key, val in child_struct.items()
                            if key in relationship_model.init_field_names()
                        }

                        try:
                            child_model_instance = relationship_model(**init_data)
                            new_row.append(child_model_instance)
                        except Exception as e:
                            if relationship_model.__name__ in self._skipped_data:
                                self._skipped_data[relationship_model.__name__].append(
                                    (child_struct, str(e))
                                )
                            else:
                                self._skipped_data[relationship_model.__name__] = [
                                    (child_struct, str(e))
                                ]

                    row[relationship_name] = new_row

        return with_relationships

    @computed_field
    @property
    def _model_instances_in_db(self) -> Sequence[Base]:
        return self.session.execute(select(self.model)).scalars().all()

    def to_db(self) -> dict[str, list[tuple[dict[str, Any], str]]]:
        for struct in self._with_parents:
            init_data = {
                key: val
                for key, val in struct.items()
                if key in self.model.init_field_names()
            }

            try:
                new_model_instance = self.model(**init_data)
            except Exception as e:
                if self.model.__name__ in self._skipped_data:
                    self._skipped_data[self.model.__name__].append((struct, str(e)))
                else:
                    self._skipped_data[self.model.__name__] = [(struct, str(e))]

                new_model_instance = None

            if (
                new_model_instance in self._model_instances_in_db
                or new_model_instance is None
            ):
                continue

            self.session.add(new_model_instance)

            self.session.flush()

        return self._skipped_data

        # df_as_models = df_as_structs.select(
        #     pl.col(f'{self.model.__name__}_struct')
        #     .map_elements(
        #         function=lambda data: get_model_instance_from_db(
        #             data, session=self.session, model=self.model
        #         )
        #     )
        #     .alias(self.model.__name__)
        # )

        # for model_instance in df_as_models.get_column(self.model.__name__):
        #     self.session.add(model_instance)
