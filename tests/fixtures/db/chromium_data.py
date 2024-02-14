from datetime import date

from pytest import fixture

from scbl_utils.db.orm.models.data import Platform
from scbl_utils.db.orm.models.entities import Lab, Person
from scbl_utils.db.orm.models.platforms.chromium import ChromiumAssay, ChromiumDataSet

from .entity_data import lab, person


@fixture
def chromium_platform() -> Platform:
    """Create a valid Platform object for testing"""
    return Platform(
        name='Chromium',
        data_set_id_length=9,
        sample_id_length=9,
        data_set_id_prefix='CD',
        sample_id_prefix='CS',
    )


@fixture
def chromium_assay() -> ChromiumAssay:
    return ChromiumAssay(name='assay')


@fixture
def chromium_data_set(
    lab: Lab, person: Person, chromium_assay: ChromiumAssay, chromium_platform: Platform
) -> ChromiumDataSet:
    ds_id = f'{chromium_platform.data_set_id_prefix}99{0:0{chromium_platform.data_set_id_length - 4}}'
    return ChromiumDataSet(
        id=ds_id,
        name='data_set',
        lab=lab,
        platform=chromium_platform,
        date_initialized=date(1999, 1, 1),
        ilab_request_id='ilab_request_id',
        submitter=person,
        assay=chromium_assay,
    )
