from collections.abc import Iterable, Iterator, Sequence
from functools import cached_property
from itertools import groupby
from typing import Annotated, Any

from pydantic import (
    AfterValidator,
    BaseModel,
    StringConstraints,
    computed_field,
    model_validator,
)
from scbl_db import ORDERED_MODELS, Base
from scbl_db.bases import Base, Data, Entity, Process
from sqlalchemy import inspect


def validate_db_target(db_target: str) -> str:
    model_name, field = db_target.split('.', maxsplit=1)

    if field.count('.') > 0:
        model = ORDERED_MODELS[model_name]
        parent_name, parent_field = field.split('.', maxsplit=1)
        if parent_name not in inspect(model).relationships.keys():
            raise ValueError(f'{parent_name} is not a relationship in {model_name}')

        return validate_db_target(db_target=field)

    elif field not in ORDERED_MODELS[model_name].field_names():
        raise ValueError(f'[orange1]{field}[/] not in [green]{model_name}[/]')

    return db_target


DBTarget = Annotated[
    str,
    StringConstraints(
        pattern=rf'({"|".join(model_name for model_name in ORDERED_MODELS)})\.[\w.]+'
    ),
    AfterValidator(validate_db_target),
]
DataRow = dict[DBTarget, Any]


class DataToInsert(
    BaseModel,
    arbitrary_types_allowed=True,
    extra='forbid',
    strict=True,
    validate_assignment=True,
    validate_default=True,
    validate_return=True,
):
    source: str
    data: Iterator[DataRow] | Sequence[DataRow]
    columns: Sequence[DBTarget]
    model: type[Base] | type[Data] | type[Entity] | type[Process]

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
    def renamed_columns(self) -> Iterable[str]:
        return tuple(self.column_renamer[col] for col in self.columns)

    @computed_field
    @cached_property
    def parent_to_columns(self) -> dict[str, tuple[str, ...]]:
        parent_columns = sorted(
            col for col in self.renamed_columns if col.count('.') > 0
        )
        # inspector = inspect(self.model)

        return {
            parent_name: tuple(columns)
            for parent_name, columns in groupby(
                parent_columns, key=lambda col: col.split('.')[0]
            )
        }

    @computed_field
    @cached_property
    def cleaned_data(self) -> Iterable[DataRow]:
        self.data = (
            {
                self.column_renamer[col]: value if value != '' else None
                for col, value in row.items()
            }
            for row in self.data
        )

        to_add = []
        for row in self.data:
            pass

        # if 'id' in self.model.required_init_field_names() and 'id' not in self.renamed_columns and 'id_dat':


# def data_to_insert(
#     source: str, data: list[dict[str, Any]], model: type[Base]
# ) -> DataToInsertBase:
#     joined_fields = '|'.join(model.field_names())
#     column_pattern = rf'{model.__name__}\.({joined_fields})(\.?[\w+])*'

#     @dataclass(frozen=True, config=ConfigDict(arbitrary_types_allowed=True))
#     class DataToInsert(DataToInsertBase):
#         source_: str
#         data_: list[dict[str, Any]]
#         model_: type[Base]
#         columns_: Collection[Annotated[str, StringConstraints(pattern=column_pattern)]]

#         def validate_target(self, parent_model: type[Base], target: str) -> None:
#             if '.' not in target:
#                 if target not in parent_model.field_names():
#                     raise ValueError(
#                         f'{target} is not a column in {parent_model.__name__} from {self.source_}'
#                     )
#                 return

#             grandparent_name, grandparent_field = target.split('.', maxsplit=1)
#             if grandparent_name not in inspect(parent_model).relationships:
#                 raise ValueError(
#                     f'{grandparent_name} is not a relationship on {parent_model.__name__}'
#                 )

#             grandparent_model = (
#                 inspect(parent_model).relationships[grandparent_name].mapper.class_
#             )

#             return self.validate_target(
#                 parent_model=grandparent_model, target=grandparent_field
#             )

#         @model_validator(mode='after')
#         def validate_columns(self: 'DataToInsert') -> 'DataToInsert':
#             for col in self.columns_:
#                 self.validate_target(parent_model=self.model_, target=col) if col.count(
#                     '.'
#                 ) > 1 else None

#             return self

#     return DataToInsert(
#         source_=source, data_=data, model_=model, columns_=data[0].keys()
#     )
