from typing import Literal

from pydantic import BaseModel


class GeometrySchema(BaseModel):

    type: Literal["Polygon"]
    coordinates: list[list[list[float | int]]]


class FeatureSchema(BaseModel):

    type: Literal["Feature"]
    geometry: GeometrySchema

