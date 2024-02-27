from pydantic import BaseModel


class StrictBaseModel(
    BaseModel,
    extra='forbid',
    frozen=True,
    strict=True,
    validate_assignment=True,
    validate_default=True,
    validate_return=True,
):
    pass
