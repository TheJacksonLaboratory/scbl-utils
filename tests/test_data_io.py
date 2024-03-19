from pathlib import Path

import polars as pl
from pytest import fixture
from scbl_db import ChromiumDataSet

from scbl_utils.data_io import DataToInsert2
from scbl_utils.main import SCBLUtils


class TestDataToInsert:
    def test_data_preparation(self, config_dir: Path):
        google_drive_data_dir = Path(__file__).parent / 'google-drive_data'

        data = pl.read_csv(
            google_drive_data_dir / 'multiplexing_sheet.csv',
            schema={
                'ChromiumDataSet.name': pl.String,
                'ChromiumDataSet.ilab_request_id': pl.String,
                'ChromiumDataSet.date_initialized': pl.Date,
                'ChromiumDataSet.assay.name': pl.String,
                'ChromiumDataSet.lab.pi.email': pl.String,
                'ChromiumDataSet.submitter.email': pl.String,
                'ChromiumDataSet.libraries.id': pl.String,
                'ChromiumDataSet.libraries.date_constructed': pl.Date,
                'ChromiumDataSet.libraries.status': pl.String,
                'ChromiumDataSet.samples.name': pl.String,
                'ChromiumDataSet.samples.date_received': pl.Date,
            },
        )

        scbl_utils = SCBLUtils(config_dir=config_dir)

        with scbl_utils._db_sessionmaker.begin() as session:
            print(
                DataToInsert2(
                    data=data, model=ChromiumDataSet, session=session, source='test'
                )._with_parents
            )

        assert False
