import json
import asyncio

import geopandas as gpd
import pandas as pd

from .dto.effects_dto import EffectsDTO
from .modules import (
    effects_api_gateway,
    data_restorator,
    attribute_parser,
    matrix_builder, objectnat_calculator
)


class EffectsService:
    """
    Class for handling services calculation
    """

    # ToDo Make in and out crs convertations here
    # Todo Ensure async
    @staticmethod
    async def calculate_effects(effects_params: EffectsDTO) -> dict[str, dict]:
        """
        Calculate provision effects by project data and target scenario
        Args:
            effects_params (EffectsDTO): Project data
        Returns:
             gpd.GeoDataFrame: Provision effects
        """

        project_data = await effects_api_gateway.get_project_data(
            effects_params.project_id
        )
        normative_data = await effects_api_gateway.get_service_normative(
            territory_id=project_data["territory"]["id"],
            service_type_id=effects_params.service_type_id,
            year=effects_params.year,
        )
        context_buildings = await effects_api_gateway.get_scenario_buildings(
            scenario_id=project_data["project"]["base_scenario"]["id"]
        )
        context_buildings = await attribute_parser.parse_living_area_from_buildings(
            living_buildings=context_buildings,
        )
        context_services = await effects_api_gateway.get_scenario_services(
            scenario_id=project_data["project"]["base_scenario"]["id"]
        )
        project_buildings = await effects_api_gateway.get_scenario_buildings(
            scenario_id=effects_params.scenario_id
        )
        project_services = await effects_api_gateway.get_scenario_services(
            scenario_id=effects_params.scenario_id
        )
        after_buildings = await asyncio.to_thread(
            pd.concat,
            [context_buildings, project_buildings]
        )
        after_services = await asyncio.to_thread(
            pd.concat,
            [context_services, project_services]
        )
        before_matrix = await asyncio.to_thread(
            matrix_builder.calculate_availability_matrix,
            buildings=context_buildings,
            context_services=context_services,
            normative_value=normative_data["normative_value"],
            normative_type=normative_data["normative_type"],
        )
        after_matrix = await asyncio.to_thread(
            matrix_builder.calculate_availability_matrix,
            buildings=after_buildings,
            context_services=context_services,
            normative_value=normative_data["normative_value"],
            normative_type=normative_data["normative_type"],
        )
        before_prove_data = objectnat_calculator.evaluate_provision(
            buildings=context_buildings,
            services=context_services,
            matrix=before_matrix,
            service_normative=normative_data["service_normative"],
        )
        after_prove_data = objectnat_calculator.evaluate_provision(
            buildings=after_buildings,
            services=after_services,
            matrix=after_matrix,
            service_normative=normative_data["service_normative"],
        )
        effects = objectnat_calculator.estimate_effects(
            before_prove_data=before_prove_data["provision"],
            after_prove_data=after_prove_data["provision"],
        )
        result = {
            "before_prove_data": {
                "provision": json.load(before_prove_data["provision"]),
                "services": json.load(before_prove_data["services"]),
                "links": json.load(before_prove_data["links"]),
            },
            "after_prove_data": {
                "provision": json.load(after_prove_data["provision"]),
                "services": json.load(after_prove_data["services"]),
                "links": json.load(after_prove_data["links"]),
            },
            "effects": json.load(effects.json()),
        }
        return result


effects_service = EffectsService()
