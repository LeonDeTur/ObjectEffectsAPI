import json
from typing import Literal

import pandas as pd
import geopandas as gpd
from objectnat import get_service_provision

from app.dependencies import http_exception


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

    # ToDo Rewrite logic to one gdf
    @staticmethod
    def _calculate_effects(
            provision_before: pd.DataFrame | gpd.GeoDataFrame,
            provision_after: pd.DataFrame | gpd.GeoDataFrame,
            prov_type: Literal["within", "without"],
            mark_type: Literal["absolute", "relative"]
    ) -> pd.Series:
        """
        Function calculates provision effects
        Args:
            provision_before (pd.DataFrame | gpd.GeoDataFrame): GeoDataFrame of provision before
            provision_after (pd.DataFrame | gpd.GeoDataFrame): GeoDataFrame of provision after
            prov_type (Literal["within", "without"]): "within" or "without" provision to estimate effects
            mark_type: "absolute" or "relative", effects the value, relative from -1 to 0, absolute in int values
        Returns:
            pd.Series: effects results
        """

        if prov_type not in ("within", "without"):
            raise http_exception(
                status_code=500,
                msg=f"Invalid provision type{prov_type}",
                _input={
                    "prov_type": prov_type,
                },
                _detail={
                    "Supported prov_types": ["within", "without"],
                }
            )
        supplied_demand_before = provision_before[f"supplied_demand_{prov_type}"]
        supplied_demand_after = provision_after[f"supplied_demand_{prov_type}"]
        unsupplied_demand_before = provision_before[f"us_demand_{prov_type}"]
        unsupplied_demand_after = provision_after[f"us_demand_{prov_type}"]
        if mark_type == "relative":
            total_population = provision_before["demand"].sum() + provision_after["demand"].sum()
            return (supplied_demand_after-supplied_demand_before) - (supplied_demand_after-supplied_demand_before) / total_population
        elif mark_type == "absolute":
            return (supplied_demand_after-supplied_demand_before).apply(lambda x: max(0, x)) - (unsupplied_demand_after-unsupplied_demand_before).apply(lambda x: max(0, x))
        else:
            raise http_exception(
                status_code=500,
                msg=f"Unsupported effects calculation for mark_type {mark_type}.",
                _input={
                    "prov_type": prov_type,
                },
                _detail={
                    "Supported mark_types": ["absolute", "relative"]
                },
            )

    # ToDo Refactor case with new and old objects (do fillna after concat/merge on unique geometry) in new function
    def estimate_effects(
            self,
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

        effects = provision_after[["geometry", "demand"]].copy()

        provision_before[
            "us_demands_within"
        ] = provision_before["demand"] - provision_before["supplied_demand_within"]
        provision_before[
            "us_demands_without"
        ] = provision_before["demand"] - provision_before["supplied_demand_without"]
        provision_after[
            "us_demands_within"
        ] = provision_after["demand"] - provision_after["supplied_demand_within"]
        provision_after[
            "us_demands_without"
        ] = provision_after["demand"] - provision_after["supplied_demand_without"]

        effects["absolute_effects_within"] = self._calculate_effects(
            provision_before=provision_before,
            provision_after=provision_after,
            prov_type="within",
            mark_type="absolute",
        )
        effects["relative_effects_within"] = self._calculate_effects(
            provision_before=provision_before,
            provision_after=provision_after,
            prov_type="within",
            mark_type="relative",
        )
        effects["absolute_effects_without"] = self._calculate_effects(
            provision_before=provision_before,
            provision_after=provision_after,
            prov_type="without",
            mark_type="absolute",
        )
        effects["relative_effects_without"] = self._calculate_effects(
            provision_before=provision_before,
            provision_after=provision_after,
            prov_type="without",
            mark_type="relative",
        )

        return effects


objectnat_calculator = ObjectNatCalculator()
