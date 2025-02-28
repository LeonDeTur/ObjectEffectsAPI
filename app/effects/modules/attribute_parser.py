import json
import asyncio

import pandas as pd
import geopandas as gpd

from app.dependencies import http_exception


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

        return data_row[0].get("stores_count")

    @staticmethod
    def _parse_buildings_id(
            data_row: list[dict]
    ) -> int:
        """
        Function parse building id from nested data
        Args:
            data_row (list[dict]): nested data response from nested buildings data response
        Returns:
            int: buildings id
        """

        if not (target_data:=data_row[0]["physical_object_id"]):
            raise http_exception(
                status_code=404,
                msg="Couldn't retrieve physical object id from db",
                _input=data_row,
                _detail={}
            )
        return target_data

    async def parse_all_from_buildings(
            self,
            living_buildings: pd.DataFrame | gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """
        Function purses living building area for buildings from nested response
        Args:
            living_buildings (gpd.GeoDataFrame): nested response from api as feature collection
        Returns:
            gpd.GeoDataFrame: living building area with parsed storeys data. Can be empty
        """

        living_buildings = living_buildings.copy()
        if living_buildings.empty:
            return living_buildings
        living_buildings["storeys_count"] = await asyncio.to_thread(
            living_buildings["physical_objects"].apply,
            self._get_stores_count,
        )
        living_buildings["building_id"] = await asyncio.to_thread(
            living_buildings["physical_objects"].apply,
            self._parse_buildings_id,
        )
        living_buildings = living_buildings.drop(
            ['object_geometry_id', 'territory', 'address', 'osm_id', 'physical_objects', 'services'],
            axis=1,
        )
        return living_buildings

    @staticmethod
    def _parse_service_capacity(
            services:gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """
        Function parses capacity attributes from nested response
        Args:
            services (gpd.GeoDataFrame): nested response from api as feature collection
        Returns:
            gpd.GeoDataFrame: service capacity with parsed storeys data. Can be empty
        """

        services["capacity"] = services["services"].apply(lambda x: x[0].get("capacity"))
        return services

    @staticmethod
    def _parse_service_id(
            services: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Function parses service id from nested response
        Args:
            services (gpd.GeoDataFrame): nested response from api as feature collection
        Returns:
            gpd.GeoDataFrame: service id with parsed storeys data. Can be empty
        """

        services["service_id"] = services["services"].apply(lambda x: x[0].get("service_id"))
        return services

    async def parse_all_from_services(
            self,
            services: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """
        Function parses all required data from service request data
        Args:
            services (gpd.GeoDataFrame): nested response from api as feature collection
        Returns:
            gpd.GeoDataFrame: service capacity with parsed storeys data. Can be empty
        """
        services = services.copy()
        if services.empty:
            return services
        services = await asyncio.to_thread(
            self._parse_service_id,
            services=services,
        )
        services = await asyncio.to_thread(
            self._parse_service_capacity,
            services=services
        )
        services = services.drop(
            ['object_geometry_id', 'territory', 'address', 'osm_id', 'physical_objects', 'services'],
            axis=1
        )
        return services

attribute_parser = AttributeParser()
