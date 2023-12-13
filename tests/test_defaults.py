from scbl_utils.defaults import CSV_SCHEMAS, DB_INIT_FILES


def test_filenames_match():
    """
    Test that the keys of `CSV_SCHEMAS` and `DB_INIT_FILES` are the same.
    """
    assert CSV_SCHEMAS.keys() == {path.name for path in DB_INIT_FILES}
