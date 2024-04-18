from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from scbl_db import (
    ChromiumDataSet,
    ChromiumLibrary,
    ChromiumLibraryType,
    Institution,
    Lab,
    Person,
)
from sqlalchemy import select

from scbl_utils.data_io import DataInserter
from scbl_utils.main import SCBLUtils


class TestDataInserter:
    def test_simple_insertion(self, cli: SCBLUtils):
        name = 'JAX'
        email_format = '{first_name}.{last_name}@jax.org'
        country = 'US'
        city = 'Farmington'
        short_name = 'JAX'

        data = {
            'Institution.name': [name],
            'Institution.email_format': [email_format],
            'Institution.country': [country],
            'Institution.city': [city],
            'Institution.short_name': [short_name],
        }
        data = pl.DataFrame(data)

        with cli._db_sessionmaker.begin() as session:
            DataInserter(
                data=data, model=Institution, session=session, source='test'
            ).to_db()

        with cli._db_sessionmaker.begin() as session:
            institution_in_db = session.execute(select(Institution)).scalar()

            assert institution_in_db == Institution(
                name=name,
                email_format=email_format,
                country=country,
                city=city,
                short_name=short_name,
            )

    def test_column_renaming(self, cli: SCBLUtils):
        data = {'Institution.name': ['JAX']}
        data = pl.DataFrame(data)

        renamed_data = pl.DataFrame({'name': ['JAX']})

        with cli._db_sessionmaker.begin() as session:
            assert_frame_equal(
                DataInserter(
                    data=data, model=Institution, session=session, source='test'
                )._renamed,
                renamed_data,
            )

    def test_relationship_to_columns(self, cli: SCBLUtils):
        data = {'Lab.name': ['foo'], 'Lab.institution.name': ['JAX']}
        data = pl.DataFrame(data)

        with cli._db_sessionmaker.begin() as session:
            relationship_to_column = DataInserter(
                data=data, session=session, model=Lab, source='test'
            )._relationship_to_columns

            assert relationship_to_column == [('institution', ['institution.name'])]

    def test_aggregation(self, cli: SCBLUtils):
        data = {
            'ChromiumDataSet.id': ['foo', 'foo', 'bar'],
            'ChromiumDataSet.lab.name': ['lab', 'lab', 'lab'],
            'ChromiumDataSet.libraries.id': ['lib', 'lib1', 'lib2'],
        }
        data = pl.DataFrame(data)

        expected_aggregation = {
            'id': ['foo', 'bar'],
            'lab.name': ['lab', 'lab'],
            'lab': [{'name': 'lab'}, {'name': 'lab'}],
            'libraries.id': [['lib', 'lib1'], ['lib2']],
            'ChromiumDataSet_index': [1, 2],
        }
        expected_aggregation = pl.DataFrame(expected_aggregation)

        with cli._db_sessionmaker.begin() as session:
            aggregated = DataInserter(
                data=data, session=session, model=ChromiumDataSet, source='test'
            )._aggregated

            assert_frame_equal(
                aggregated,
                expected_aggregation,
                check_column_order=False,
                check_dtype=False,
            )

    def test_id_assignment(self, cli: SCBLUtils):
        data = {'ChromiumDataSet.date_initialized': [date(1999, 1, 1)]}
        data = pl.DataFrame(data)

        expected_output = {
            'date_initialized': [date(1999, 1, 1)],
            'id': ['SD9900001'],
            'ChromiumDataSet_index': [1],
        }
        expected_output = pl.DataFrame(expected_output)

        with cli._db_sessionmaker.begin() as session:
            assert_frame_equal(
                DataInserter(
                    data=data, session=session, model=ChromiumDataSet, source='test'
                )._with_id,
                expected_output,
                check_column_order=False,
                check_dtype=False,
            )

    def test_parent_assignment(self, cli: SCBLUtils):
        data = {'Person.first_name': ['ahmed'], 'Person.institution.name': ['JAX']}
        data = pl.DataFrame(data)

        with cli._db_sessionmaker.begin() as session:
            institution = Institution(
                name='JAX',
                short_name='JAX',
                country='US',
                email_format='{first_name}.{last_name}@jax.org',
                city='Farmington',
            )

            session.add(institution)

        with cli._db_sessionmaker.begin() as session:
            data_with_parents = DataInserter(
                data=data, model=Person, session=session, source='test'
            )._with_parents
            institution = session.execute(select(Institution)).scalar()

            expected_data = {
                'first_name': ['ahmed'],
                'institution': [institution],
                'Person_index': [1],
                'institution.name': ['JAX'],
            }
            expected_data = pl.DataFrame(
                expected_data,
                schema={
                    'first_name': pl.String,
                    'institution': pl.Object,
                    'Person_index': pl.UInt32,
                    'institution.name': pl.String,
                },
            )

            # Compare dicts because polars doesn't support assert_frame_equal for arbitrary objects
            assert expected_data.to_dicts() == data_with_parents.to_dicts()

    def test_children_records(self, cli: SCBLUtils):
        data = {
            'ChromiumDataSet.date_initialized': [date(1999, 1, 1)],
            'ChromiumDataSet.libraries.id': ['SC9900001'],
            'ChromiumDataSet.libraries.library_type.name': ['foo'],
        }
        data = pl.DataFrame(data)

        with cli._db_sessionmaker.begin() as session:
            library_type = ChromiumLibraryType(name='foo')
            session.add(library_type)

        with cli._db_sessionmaker.begin() as session:
            library_type = session.execute(select(ChromiumLibraryType)).scalar()

            if library_type is None:
                pytest.fail('ChromiumLibraryType not added to database successfully')

            with_children = DataInserter(
                data=data, session=session, source='test', model=ChromiumDataSet
            )._with_children_as_records

            expected_result = [
                {
                    'id': 'SD9900001',
                    'date_initialized': date(1999, 1, 1),
                    'libraries': [
                        ChromiumLibrary(id='SC9900001', library_type=library_type)
                    ],
                    'ChromiumDataSet_index': 1,
                    'libraries.id': ['SC9900001'],
                    'libraries.library_type.name': ['foo'],
                }
            ]

            assert with_children == expected_result

    def test_duplicate_data_insertion(self, cli: SCBLUtils):
        name = 'JAX'
        email_format = '{first_name}.{last_name}@jax.org'
        country = 'US'
        city = 'Farmington'
        short_name = 'JAX'

        data = {
            'Institution.name': [name, name],
            'Institution.email_format': [email_format, email_format],
            'Institution.country': [country, country],
            'Institution.city': [city, city],
            'Institution.short_name': [short_name, short_name],
        }
        data = pl.DataFrame(data)

        with cli._db_sessionmaker.begin() as session:
            DataInserter(
                data=data, session=session, source='test', model=Institution
            ).to_db()

        with cli._db_sessionmaker.begin() as session:
            institutions = session.execute(select(Institution)).scalars().all()

            assert len(institutions) == 1

    def test_column_name_not_in_db(self, cli: SCBLUtils):
        data = {'foo': [1, 2, 3]}
        data = pl.DataFrame(data)

        with cli._db_sessionmaker.begin() as session:
            with pytest.raises(ValueError):
                DataInserter(
                    data=data, model=Institution, session=session, source='test'
                ).to_db()

    def test_column_name_not_match_model(self, cli: SCBLUtils):
        data = {'Lab.name': ['foo']}
        data = pl.DataFrame(data)

        with cli._db_sessionmaker.begin() as session:
            with pytest.raises(ValueError):
                DataInserter(
                    data=data, model=Institution, session=session, source='test'
                )
