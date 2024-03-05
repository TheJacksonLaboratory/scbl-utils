from pydantic import BaseModel, ConfigDict

strict_config = ConfigDict(
    arbitrary_types_allowed=True,
    extra='forbid',
    frozen=True,
    validate_assignment=True,
    validate_default=True,
    validate_return=True,
)


class StrictBaseModel(BaseModel, frozen=True):
    model_config = strict_config
