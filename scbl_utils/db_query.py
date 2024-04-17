from collections.abc import Sequence
from typing import Any

from pydantic import ConfigDict, validate_call
from scbl_db import Base
from sqlalchemy import inspect, select
from sqlalchemy.orm import Mapper, Session


def construct_where_condition(
    attribute_name: str, value: Any, model_mapper: Mapper[Base]
):
    if '.' not in attribute_name:
        if attribute_name not in model_mapper.attrs.keys():
            return

        attribute = model_mapper.attrs[attribute_name].class_attribute
        return attribute.ilike(value) if isinstance(value, str) else attribute == value

    parent_name, parent_attribute_name = attribute_name.split('.', maxsplit=1)
    parent_mapper = model_mapper.relationships[parent_name].mapper

    parent_where_condition = construct_where_condition(
        parent_attribute_name, value, model_mapper=parent_mapper
    )

    parent = model_mapper.attrs[parent_name].class_attribute
    return parent.has(parent_where_condition)


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def get_model_instance_from_db(
    data: dict[str, Any], session: Session, model: type[Base]
) -> Sequence[Base] | Base | None:
    model_mapper = inspect(model)

    where_conditions = []

    for col, val in data.items():
        if val is None:
            continue

        where = construct_where_condition(col, value=val, model_mapper=model_mapper)

        if where is None:
            continue

        where_conditions.append(where)

    if not where_conditions:
        return None

    stmt = select(model_mapper).where(*where_conditions)
    matches = session.execute(stmt).scalars().all()

    if len(matches) == 0:
        return

    if len(matches) == 1:
        return matches[0]

    return matches
