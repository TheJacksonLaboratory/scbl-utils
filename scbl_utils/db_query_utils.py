from collections.abc import Sequence
from functools import cache
from typing import Any

from pydantic import ConfigDict, validate_call
from scbl_db import Base
from sqlalchemy import select
from sqlalchemy.orm import Mapper, Session


@cache
def construct_where_condition(
    attribute_name: str, value: Any, model_mapper: Mapper[Base]
):
    if '.' not in attribute_name:
        attribute = model_mapper.attrs[attribute_name].class_attribute
        return attribute.ilike(value) if isinstance(value, str) else attribute == value

    parent_name, parent_attribute_name = attribute_name.split('.', maxsplit=1)
    parent_mapper = model_mapper.relationships[parent_name].mapper

    parent_where_condition = construct_where_condition(
        parent_attribute_name, value, model_mapper=parent_mapper
    )

    parent = model_mapper.attrs[parent_name].class_attribute
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
