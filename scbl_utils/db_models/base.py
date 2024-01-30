"""
This module contains base class for SQLAlchemy models and modified type
definitions for use in the models defined in `data.py` and 
`definitions.py`.

Classes:
    - `Base`: Base class for SQLAlchemy models
    
    - `StrippedString`: A string type that strips whitespace from the ends
    of the string before sending to the database
    
    - `SamplesheetString`: A string type that removes illegal characters
    from the string before sending to the database. Suitable for strings
    that will be used in the samplesheet passed as input to the `nf-tenx`
    pipeline
"""
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass


class Base(MappedAsDataclass, DeclarativeBase, kw_only=True):
    @classmethod
    def get_model(cls, tablename: str) -> type['Base']:
        tablename_to_model = {
            model.__tablename__: model for model in cls.__subclasses__()
        }
        model = tablename_to_model.get(tablename)

        if model is None:
            raise KeyError(
                f'{tablename} is not a valid table name. Make sure you have imported all database models in the code that calls this function.'
            )

        return model
