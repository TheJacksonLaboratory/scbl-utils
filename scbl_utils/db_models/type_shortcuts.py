from re import sub
from typing import Annotated

from sqlalchemy.orm import mapped_column
from sqlalchemy.types import String, TypeDecorator

from scbl_utils.defaults import SAMPLENAME_BLACKLIST_PATTERN, SEP_PATTERN


class StrippedString(TypeDecorator):
    """
    A string type that strips whitespace from the ends of the string
    before sending to the database.
    """

    impl = String
    cache_ok = True

    def process_bind_param(self, string: str | None, dialect: str) -> str | None:
        return string.strip() if string is not None else string


class SamplesheetString(TypeDecorator):
    """
    A string type that removes illegal characters from the string before
    sending to the database. Suitable for strings that will be used in
    the samplesheet passed as input to the `nf-tenx` pipeline.
    """

    impl = String
    cache_ok = True

    def process_bind_param(self, string: str, dialect: str) -> str | None:
        if string is None:
            return string

        # Remove illegal characters
        string = sub(
            pattern=SAMPLENAME_BLACKLIST_PATTERN, repl='', string=string.strip()
        )

        # Replace separator characters with hyphens
        string = sub(pattern=SEP_PATTERN, repl='-', string=string)

        # Replace occurrences of multiple hyphens with a single hyphen
        string = sub(pattern=r'-+', repl='-', string=string)

        return string


# Commonly used primary key types
int_pk = Annotated[int, mapped_column(primary_key=True)]
stripped_str_pk = Annotated[str, mapped_column(StrippedString, primary_key=True)]
samplesheet_str_pk = Annotated[str, mapped_column(SamplesheetString, primary_key=True)]

# Commonly used non-unique string types
stripped_str = Annotated[str, mapped_column(StrippedString)]
samplesheet_str = Annotated[str, mapped_column(SamplesheetString)]

# Commonly used unique string types
unique_stripped_str = Annotated[str, mapped_column(StrippedString, unique=True)]
unique_samplesheet_str = Annotated[str, mapped_column(SamplesheetString, unique=True)]
