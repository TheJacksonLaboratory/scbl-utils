"""
This module contains SQLAlchemy models for the `scbl-utils` package.
These models represent the definitions of experimental data stored in
the database, as opposed to the data itself, which is stored in
`data.py`. For example, an `Experment` is really an instance of a
`Platform`.

Classes:
    - `Platform`: Represents an experimental protocol or platform.

    - `LibraryType`: Represents a cDNA library type, such as gene
    expression or chromatin accessibility

    - `Tag`: Represents a tag used to multiplex `Sample`s in a
    `Library`.
"""
