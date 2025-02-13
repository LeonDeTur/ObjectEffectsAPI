from typing import Literal

import pandas as pd
import geopandas as gpd
from aiohttp import http_exceptions

from app.dependencies import http_exception


class DemandRestorator:

    @staticmethod
    def _restore_target_population(
            buildings: gpd.GeoDataFrame,
    ) -> int:
        """
        Function estimates target population for territory
        Args:
            buildings (gpd.GeoDataFrame): living buildings data
        Returns:
            int: target population to restore
        """

        local_crs = buildings.estimate_utm_crs()
        buildings = buildings.to_crs(local_crs)
        return sum(buildings.area * buildings["storey_counts"]) * 0.8/33

    @staticmethod
    def _restore_demands(
            buildings: gpd.GeoDataFrame,
            service_normative: int,
            service_normative_type: Literal["", ""]
    ) -> gpd.GeoDataFrame:
        """
        Function restores demands for service
        Args:
            service_normative (int): service normative
            service_normative_type (str): service normative type
        Returns:
            gdp.GeoDataFrame: buildings data with restored demands
        """

        if service_normative_type == "":
            return buildings
        elif service_normative_type == "":
            return buildings
        else:
            raise http_exception(
                status_code=400,
                msg="Service demand normative not found",
                _input={
                    "service_normative_type": service_normative_type,
                },
                _detail={
                    "available_demand_type": [
                        "", ""
                    ]
                }
            )

    def restore_population(
            self,
            buildings: gpd.GeoDataFrame,
            target_population: int | None = None,

    ):
        """
        Function fills population data with objectnat population restoration
        Args:
            buildings (gpd.GeoDataFrame): living buildings data
            target_population (int | None): Target population to restore, defaults to None
        """

        if not target_population:
            target_population = self._restore_target_population(buildings)

