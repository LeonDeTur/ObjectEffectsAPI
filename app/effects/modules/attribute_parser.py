import json
import asyncio

import pandas as pd
import geopandas as gpd


class AttributeParser:
    """
    Cass aimed to parse data in acceptable format to convert in other dtypes
    """

    @staticmethod
    def _get_stores_count(
            data_row: list[dict]
    ) -> int | None:
        """
        Function gets stores count data from nested buildings data response
        Args:
            data_row (list[dict]): nested data response from nested buildings data response
        Returns:
             int | None: storeys count number or None if Not found
        """

        if not (target_data:=data_row[0]["living_building"]):
            return None
        target_data = target_data["properties"].get("building_data")
        if not target_data:
            return None
        stores_count = json.loads(target_data).get("storeys_count")
        return stores_count

    async def parse_living_area_from_buildings(
            self,
            living_buildings: pd.DataFrame | gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """
        Function purses living building area for buildings from nested response
        Args:
            living_buildings (gpd.GeoDataFrame): nested response from api as feature collection
        Returns:
            gpd.GeoDataFrame: living building area with parsed storeys data
        """

        living_buildings["storeys_count"] = asyncio.to_thread(
            self._get_stores_count,
            living_buildings,
        )
        return living_buildings


attribute_parser = AttributeParser()
