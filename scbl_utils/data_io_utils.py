from abc import ABC, abstractmethod
from collections.abc import Collection
from typing import Annotated, Any

from pydantic import ConfigDict, StringConstraints, model_validator
from pydantic.dataclasses import dataclass
from scbl_db import Base
from sqlalchemy import inspect


class DataToInsertBase(ABC):
    source_: str
    data_: list[dict[str, Any]]
    model_: type[Base]
    columns_: Collection[str]

    @abstractmethod
    def validate_target(self, parent_model: type[Base], target: str) -> None:
        pass

    @abstractmethod
    def validate_columns(self) -> 'DataToInsertBase':
        pass


def data_to_insert(
    source: str, data: list[dict[str, Any]], model: type[Base]
) -> DataToInsertBase:
    joined_fields = '|'.join(model.field_names())
    column_pattern = rf'{model.__name__}\.({joined_fields})(\.?[\w+])*'

    @dataclass(frozen=True, config=ConfigDict(arbitrary_types_allowed=True))
    class DataToInsert(DataToInsertBase):
        source_: str
        data_: list[dict[str, Any]]
        model_: type[Base]
        columns_: Collection[Annotated[str, StringConstraints(pattern=column_pattern)]]

        def validate_target(self, parent_model: type[Base], target: str) -> None:
            if '.' not in target:
                if target not in parent_model.field_names():
                    raise ValueError(
                        f'{target} is not a column in {parent_model.__name__} from {self.source_}'
                    )
                return

            grandparent_name, grandparent_field = target.split('.', maxsplit=1)
            if grandparent_name not in inspect(parent_model).relationships:
                raise ValueError(
                    f'{grandparent_name} is not a relationship on {parent_model.__name__}'
                )

            grandparent_model = (
                inspect(parent_model).relationships[grandparent_name].mapper.class_
            )

            return self.validate_target(
                parent_model=grandparent_model, target=grandparent_field
            )

        @model_validator(mode='after')
        def validate_columns(self: 'DataToInsert') -> 'DataToInsert':
            for col in self.columns_:
                self.validate_target(parent_model=self.model_, target=col) if col.count(
                    '.'
                ) > 1 else None

            return self

    return DataToInsert(
        source_=source, data_=data, model_=model, columns_=data[0].keys()
    )
