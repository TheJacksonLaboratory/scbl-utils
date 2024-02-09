from typing import TypeVar

from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass


class Base(MappedAsDataclass, DeclarativeBase):
    pass


Model = TypeVar('Model', bound=Base)
