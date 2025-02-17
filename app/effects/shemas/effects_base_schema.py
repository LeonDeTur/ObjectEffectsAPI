from typing import Literal, Optional, Any

from pydantic import BaseModel


class GeometrySchema(BaseModel):

    type: Literal["Polygon", "MultiPolygon", "LineString", "MultiLineString", "Point", "MultiPoint"]
    coordinates: list[Any]


class FeatureSchema(BaseModel):

    id: Optional[int | None]
    type: Literal["Feature"]
    geometry: GeometrySchema
    properties: dict


class FeatureCollectionSchema(BaseModel):

    type: Literal["FeatureCollection"]
    features: list[FeatureSchema]


class ProvisionSchema(BaseModel):

    provision: FeatureCollectionSchema
    services: FeatureCollectionSchema
    links: FeatureCollectionSchema


class EffectsSchema(BaseModel):

    provision_before: ProvisionSchema
    provision_after: ProvisionSchema
    effects: FeatureCollectionSchema
