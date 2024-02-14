from sqlalchemy.orm import Session, sessionmaker

import scbl_utils.db.helpers as db_helpers
from scbl_utils.db.orm.models.entities import Institution, Person
from scbl_utils.db.orm.models.platforms.chromium import ChromiumDataSet

from ..fixtures.db.chromium_data import (
    chromium_assay,
    chromium_data_set,
    chromium_platform,
)
from ..fixtures.db.entity_data import delivery_parent_dir, institution, lab, person
from ..fixtures.db.utils import tmp_db_path, tmp_db_session


class TestGetMatchingObj:
    """Tests for the get_matching_obj function."""

    def test_match(
        self, tmp_db_session: sessionmaker[Session], institution: Institution
    ):
        data = {'name': institution.name}

        with tmp_db_session.begin() as session:
            session.add(institution)
            retrieved_institution = db_helpers.get_matching_obj(
                data, session=session, model=Institution
            )
            assert retrieved_institution == institution

    def test_parent_match(self, tmp_db_session: sessionmaker[Session], person: Person):
        data = {
            'first_name': person.first_name,
            'last_name': person.last_name,
            'institution.name': person.institution.name,
        }

        with tmp_db_session.begin() as session:
            session.add(person)
            retrieved_person = db_helpers.get_matching_obj(
                data, session=session, model=Person
            )
            assert retrieved_person == person

    def test_grandparent_match(
        self, tmp_db_session: sessionmaker[Session], chromium_data_set: ChromiumDataSet
    ):
        data = {
            'id': chromium_data_set.id,
            'lab.pi.first_name': chromium_data_set.lab.pi.first_name,
            'lab.pi.last_name': chromium_data_set.lab.pi.last_name,
        }

        with tmp_db_session.begin() as session:
            session.add(chromium_data_set)
            retrieved_data_set = db_helpers.get_matching_obj(
                data, session=session, model=ChromiumDataSet
            )
            assert retrieved_data_set == chromium_data_set
