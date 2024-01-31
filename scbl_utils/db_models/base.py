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
from sqlalchemy import inspect
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass


class Base(MappedAsDataclass, DeclarativeBase, kw_only=True):
    @classmethod
    def get_model(cls, model_name: str) -> type['Base']:
        name_to_model = {
            model.class_.__name__: model.class_ for model in cls.registry.mappers
        }
        model = name_to_model.get(model_name)

        if model is None:
            raise KeyError(
                f'{model_name} is not a valid model name. Make sure you have imported all database models in the code that calls this function. Valid model names: {(set(name_to_model.keys()))}'
            )

        return model
