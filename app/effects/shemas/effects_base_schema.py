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

    buildings: FeatureCollectionSchema
    services: FeatureCollectionSchema
    links: FeatureCollectionSchema

class PivotSchema(BaseModel):

        sum_absolute_total: int
        average_absolute_total: int | float
        median_absolute_total: int
        average_index_total: int | float
        median_index_total: int
        sum_absolute_scenario_project: int
        average_absolute_scenario_project: int | float
        median_absolute_scenario_project: int
        average_index_scenario_project: int | float
        median_index_scenario_project: int
        sum_absolute_within: int
        average_absolute_within: int | float
        median_absolute_within: int
        sum_average_absolute_without: int
        average_absolute_without: int | float
        median_absolute_without: int


class EffectsSchema(BaseModel):

    before_prove_data: ProvisionSchema
    after_prove_data: ProvisionSchema
    effects: FeatureCollectionSchema
    pivot: PivotSchema
