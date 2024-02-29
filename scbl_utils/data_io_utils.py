from collections.abc import Generator, Iterable, Sequence
from functools import cache, cached_property
from itertools import groupby
from pathlib import Path
from typing import Annotated, Any

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    StringConstraints,
    computed_field,
    model_validator,
    validate_call,
)
from scbl_db import ORDERED_MODELS, Base
from scbl_db.bases import Base
from sqlalchemy import inspect, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapper, Session


@cache
def validate_model_field(model: type[Base], field: str):
    if field.count('.') == 0:
        if field not in model.field_names():
            raise ValueError(f'[orange1]{field}[/] not in [green]{model.__name__}[/]')

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
DataRowDict = dict[DBTarget, Any]


@cache
def construct_where_condition(attribute_name: str, value, model_mapper: Mapper[Base]):
    if '.' not in attribute_name:
        attribute = model_mapper.attrs[attribute_name].class_attribute
        return attribute.ilike(value) if isinstance(value, str) else attribute == value

    parent_name, parent_attribute_name = attribute_name.split('.', maxsplit=1)
    try:
        parent_mapper = model_mapper.relationships[parent_name].mapper
    except KeyError:
        print(
            parent_name,
            parent_attribute_name,
            model_mapper.relationships.keys(),
            model_mapper,
            sep='\n',
        )
        raise Exception
    parent = model_mapper.attrs[parent_name].class_attribute

    parent_where_condition = construct_where_condition(
        parent_attribute_name, value, model_mapper=parent_mapper
    )
    return parent.has(parent_where_condition)


@cache
@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def get_matching_obj(
    columns: tuple[str, ...], row: tuple, session: Session, model_mapper: Mapper[Base]
) -> Sequence[Base]:
    where_conditions = []

    for col, val in zip(columns, row, strict=True):
        where = construct_where_condition(col, value=val, model_mapper=model_mapper)
        where_conditions.append(where)

    if not where_conditions:
        return []

    stmt = select(model_mapper).where(*where_conditions)
    matches = session.execute(stmt).scalars().all()

    return matches


class DataToInsert(
    BaseModel,
    arbitrary_types_allowed=True,
    extra='forbid',
    strict=True,
    validate_assignment=True,
    validate_default=True,
    validate_return=True,
):
    source: str | Path
    data: Iterable[DataRowDict]
    columns: Sequence[DBTarget]
    model: type[Base]
    session: Session

    @model_validator(mode='after')
    def validate_data_columns(self: 'DataToInsert') -> 'DataToInsert':
        if not all(col.startswith(self.model.__name__) for col in self.columns):
            raise ValueError(
                f'All column names in [orange]{self.source}[/] must start with [green]{self.model.__name__}[/]'
            )

        return self

    @computed_field
    @cached_property
    def column_renamer(self) -> dict[DBTarget, str]:
        return {
            col: col.removeprefix(f'{self.model.__name__}.') for col in self.columns
        }

    @computed_field
    @cached_property
    def parent_to_original_columns(self) -> dict[str, tuple[str, ...]]:
        parent_columns = sorted(
            self.column_renamer[col] for col in self.columns if col.count('.') > 1
        )

        return {
            parent_name: tuple(columns)
            for parent_name, columns in groupby(
                parent_columns, key=lambda col: col.split('.')[0]
            )
        }

    @computed_field
    @cached_property
    def model_instances_in_db(self) -> Sequence[Base]:
        return self.session.execute(select(self.model)).scalars().all()

    @computed_field
    @property
    def cleaned_data(self) -> Generator[dict[str, Any | None], None, None]:
        return (
            {
                self.column_renamer[col]: value if value != '' else None
                for col, value in row.items()
            }
            for row in self.data
        )

    def assign_parents(
        self,
        row: dict[str, Any],
        parent_mapper: Mapper[Base],
        parent_columns: tuple[str, ...],
    ) -> Sequence[Base]:
        parent_data = {col: row[col] for col in parent_columns if row[col] is not None}

        relative_column_names = tuple(col.split('.')[1] for col in parent_data.keys())
        parent_row = tuple(parent_data.values())

        return get_matching_obj(
            columns=relative_column_names,
            row=parent_row,
            session=self.session,
            model_mapper=parent_mapper,
        )

    def to_db(self) -> None:
        relationships = inspect(self.model).relationships

        for row in self.cleaned_data:
            for parent_name, parent_columns in self.parent_to_original_columns.items():
                if parent_name in row:
                    continue

                parent_mapper = parent_mapper = relationships[parent_name].mapper
                parents = self.assign_parents(
                    row, parent_mapper=parent_mapper, parent_columns=parent_columns
                )

                if len(parents) != 1:
                    if parent_name in self.model.required_init_field_names():
                        break
                    else:
                        row[parent_name] = None

                else:
                    row[parent_name] = parents[0]

            available_init_fields = row.keys() & self.model.init_field_names()

            if available_init_fields < self.model.required_init_field_names():
                continue

            model_instance = self.model(
                **{field: row[field] for field in available_init_fields}
            )
            if (
                model_instance in self.model_instances_in_db
                or model_instance in self.session
            ):
                continue

            try:
                self.session.add(model_instance)
                self.session.flush()
            except IntegrityError:
                continue
