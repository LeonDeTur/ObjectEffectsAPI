from typing import Literal

import geopandas as gpd
from objectnat import 


class ObjectNatCalculator:

    @staticmethod
    def estimate_effects(
            buildings_before: gpd.GeoDataFrame,
            services_before: gpd.GeoDataFrame,
            buildings_after: gpd.GeoDataFrame,
            services_after: gpd.GeoDataFrame,
            normative_value: float,
            normative_type: Literal["time", "dist"]
    ) -> gpd.GeoDataFrame:
        """
        Main function which calculates provision and estimates effects
        Args:
            buildings_before (gpd.GeoDataFrame): layer with buildings before effects
            services_before: (gpd.GeoDataFrame): layer with services before effects
            buildings_after: (gpd.GeoDataFrame): layer with buildings after effects
            services_after: (gpd.GeoDataFrame): layer with services after effects
            normative_value: (int): normative time or dist to cut
            normative_type: (Literal["time", "dist"]): normative type
        Returns:
            gpd.GeoDataFrame: layer with effects, provision before and after attributes
        """

        provision_before,
