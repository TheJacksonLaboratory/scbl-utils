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
from .validated_types import DBTarget, _validate_db_target

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
    def _aggregated(self) -> pl.DataFrame:
        expressions = []

        for relationship_name, column_list in self._relationship_to_columns:
            expression = (
                pl.struct(column_list)
                .alias(relationship_name)
                .struct.rename_fields(
                    [col.removeprefix(f'{relationship_name}.') for col in column_list]
                )
            )

            if (
                self._relationships[relationship_name].direction
                == RelationshipDirection.MANYTOONE
            ):
                expression = expression.first()

            expressions.append(expression)

        aggregated = self._renamed.group_by(self._self_columns).agg(*expressions)
        return aggregated.with_row_index(offset=1)

    @computed_field
    @cached_property
    def _with_ids(self) -> pl.DataFrame:
        if (
            'id' not in self.model.init_field_names()
            or 'id' in self._aggregated.columns
        ):
            return self._aggregated

        # This line is just for IDE support
        if not issubclass(self.model, Data):
            return self._aggregated

        year_indicator_length = 2
        pad_length = (
            self.model.id_length - len(self.model.id_prefix) - year_indicator_length
        )

        id_expression = (
            self.model.id_prefix
            + pl.col(self.model.id_date_col).dt.to_string('%y')
            + pl.col('index').cast(str).str.pad_start(pad_length, fill_char='0')
        )
        return self._aggregated.with_columns(id_expression.alias('id'))

    @computed_field
    @cached_property
    # TODO: this is pretty bad. Eventually refactor and make it more efficient
    def _with_children_ids(self) -> pl.DataFrame:
        with_children_ids = self._with_ids

        for relationship_name, column_list in self._relationship_to_columns:
            relationship = self._relationships[relationship_name]
            relationship_model = relationship.mapper.class_

            if (
                relationship.direction == RelationshipDirection.MANYTOONE
                or not issubclass(relationship_model, Data)
            ):
                continue

            last_id_number = 1
            updated_relationship_column = []

            for child_struct_list in self._with_ids.get_column(relationship_name):
                child_struct_list: list[dict]

                id_date_col_idx = [
                    i
                    for i, field in enumerate(child_struct_list[0].keys())
                    if field == relationship_model.id_date_col
                ][0]

                unique_sorted_struct_as_tuples = sorted(
                    {tuple(struct.items()) for struct in child_struct_list},
                    key=lambda tup: tup[id_date_col_idx],
                )
                structs_with_ids = []

                for struct_tuple in unique_sorted_struct_as_tuples:
                    struct = dict(struct_tuple)

                    if not isinstance(struct[relationship_model.id_date_col], date):
                        continue

                    year_indicator_length = 2
                    pad_length = (
                        relationship_model.id_length
                        - len(relationship_model.id_prefix)
                        - year_indicator_length
                    )

                    if 'id' in struct:
                        structs_with_ids.append(struct)
                        continue

                    struct['id'] = (
                        relationship_model.id_prefix
                        + str(struct[relationship_model.id_date_col].strftime('%y'))
                        + f'{last_id_number:0{pad_length}}'
                    )

                    structs_with_ids.append(struct)
                    last_id_number += 1

                updated_relationship_column.append(structs_with_ids)

            new_relationship_column = pl.Series(
                name=relationship_name, values=updated_relationship_column
            )

            with_children_ids = with_children_ids.with_columns(new_relationship_column)

        return with_children_ids

    @computed_field
    @cached_property
    def _with_relationships(self) -> list[dict[str, Any]]:
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
        for struct in self._with_relationships:
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
