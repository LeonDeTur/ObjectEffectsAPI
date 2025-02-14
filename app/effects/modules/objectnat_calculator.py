from typing import Literal

import pandas as pd
import geopandas as gpd
from objectnat import get_service_provision


class ObjectNatCalculator:

    @staticmethod
    def evaluate_provision(
            buildings: gpd.GeoDataFrame,
            services: gpd.GeoDataFrame,
            matrix: pd.DataFrame,
            service_normative: int
    ) -> dict[str, gpd.GeoDataFrame]:
        """
        Function calculates provision and writes results as dict with fields "buildings", "services" and "links"
        Args:
            buildings (gpd.GeoDataFrame): GeoDataFrame of buildings
            services (gpd.GeoDataFrame): GeoDataFrame of services
            matrix (pd.DataFrame): GeoDataFrame of matrix
            service_normative (int): service normative accessibility
        Returns:
            dict[str, gpd.GeoDataFrame]: dict with fields "buildings", "services" and "links"
        """

        build_prov, services_prov, links_prov = get_service_provision(
            buildings=buildings,
            services=services,
            adjacency_matrix=matrix,
            threshold=service_normative,
        )

        return {
            "buildings": buildings,
            "services": services,
            "links": links_prov,
        }

    @staticmethod
    def estimate_effects(
            provision_before: gpd.GeoDataFrame,
            provision_after:gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """
        Main function which calculates provision and estimates effects
        Args:
            provision_before (gpd.GeoDataFrame): Provision before
            provision_after (gpd.GeoDataFrame): Provision after
        Returns:
            gpd.GeoDataFrame: layer with effects, provision before and after attributes
        """



