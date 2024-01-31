from sqlalchemy import Column, ForeignKey, Table

from .base import Base

project_person_mapping = Table(
    'project_person_mapping',
    Base.metadata,
    Column('project_id', ForeignKey('project.id'), primary_key=True),
    Column('person_id', ForeignKey('person.id'), primary_key=True),
)
