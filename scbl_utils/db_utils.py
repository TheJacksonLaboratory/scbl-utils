from scbl_db import Base
from sqlalchemy import inspect, select
from sqlalchemy.orm import Mapper, Session


def construct_where_condition(
    attribute_name: str, value, model_inspector: Mapper[Base]
):
    if '.' not in attribute_name:
        attribute = model_inspector.attrs[attribute_name].class_attribute
        return attribute.ilike(value) if isinstance(value, str) else attribute == value

    parent_name, parent_attribute_name = attribute_name.split('.', maxsplit=1)
    parent_inspector = model_inspector.relationships[parent_name].mapper
    parent = model_inspector.attrs[parent_name].class_attribute

    parent_where_condition = construct_where_condition(
        parent_attribute_name, value, model_inspector=parent_inspector
    )
    return parent.has(parent_where_condition)


def get_matching_obj(
    row: dict, session: Session, model: type[Base]
) -> Base | None | bool:
    where_conditions = []

    cleaned_row = {col: val for col, val in row.items() if val is not None}

    for col, val in cleaned_row.items():
        inspector = inspect(model)
        where = construct_where_condition(col, value=val, model_inspector=inspector)
        where_conditions.append(where)

    if not where_conditions:
        return None

    stmt = select(model).where(*where_conditions)
    matches = session.execute(stmt).scalars().all()

    if len(matches) == 0:
        return None
    elif len(matches) > 1:
        return False

    return matches[0]
