import pandas as pd
import geopandas as gpd

from app.dependencies import urban_api_handler, http_exception


class EffectsAPIGateway:

    @staticmethod
    async def get_service_normative(
            territory_id: int,
            service_type_id: int,
            year: int = 2024,
    ) -> dict[str, int | str]:
        """
        Function retrieves normative data from urban_api
        Args:
            territory_id: territory id to get normative from
            service_type_id: service to get normative from
            year: year to get normative from
        Returns:
            dict[str, int | str]: normative data with normative value and normative type (Literal["time", "dist"])
        Raises:
            400, http exception id not found
        """

        response = urban_api_handler.get(
            f"/api/v1/territory/{territory_id}/normatives",
            params={
                "year": year,
            }
        )
        for service_type in response.json():
            if service_type["service_type"]["id"] == service_type_id:
                if normative_value:=service_type["radius_availability_meters"]:
                    return {
                        "normative_value": normative_value,
                        "normative_type": "dist"
                    }
                elif normative_value:=service_type["time_availability_minutes"]:
                    return {
                        "normative_value": normative_value,
                        "normative_type": "time"
                    }
                else:
                    raise http_exception(
                        status_code=400,
                        msg="Service type normative not found",
                        _input={"service_type_id": service_type_id},
                        _detail={
                            "Available service ids": [service_type["id"] for service_type in response.json()]
                        },
                    )
        raise http_exception(
            status_code=400,
            msg="Service type normative not found",
            _input={"service_type_id": service_type_id},
            _detail={
                "Available service ids": [service_type["id"] for service_type in response.json()]
            }
        )

    @staticmethod
    async def get_project_data(project_id: int) -> dict[str, int | dict]:
        """
        Function retrieves project territory data from urban_api
        Args:
            project_id: project id to get territory from
        Returns:
            dict with "geometry" field as dict with "type" and "coordinates" fields and field "base_scenario_id"
        """

        response = urban_api_handler.get(
            f"api/v1/projects/{project_id}/territory",
        )

        return{
            "base_scenario_id": response["base_scenario"],
            "geometry": response["geometry"],
        }

    @staticmethod
    async def get_scenario_buildings(
            scenario_id: int,
    ) -> dict:
        """
        Function retrieves scenario buildings data from urban_api
        Args:
            scenario_id: scenario id to get buildings from
        Returns:
            dict: FeatureCollection of objects
        """


