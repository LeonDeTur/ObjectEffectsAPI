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
            endpoint_url=f"api/v1/projects/{project_id}/territory",
        )

        return {
            "base_scenario_id": response["base_scenario"],
            "geometry": response["geometry"],
        }

    @staticmethod
    async def get_scenario_buildings(
            scenario_id: int,
    ) -> gpd.GeoDataFrame:
        """
        Function retrieves scenario buildings data from urban_api
        Args:
            scenario_id: scenario id to get buildings from
        Returns:
            gpd.GeoDataFrame: buildings layer
        Raises:
            404, http exception living buildings not found
        """

        buildings = await urban_api_handler.get(
            endpoint_url=f"/api/v1/scenarios/{scenario_id}/geometries_with_all_objects",
            params={
                "physical_object_id": 4
            }
        )
        buildings_gdf = gpd.GeoDataFrame.from_features(buildings, crs=4326)
        if buildings_gdf.empty:
            raise http_exception(
                status_code=404,
                msg="No living buildings found",
                _input={
                    "scenario_id": scenario_id
                },
                _detail={
                    "living_buildings_data": buildings
                }
            )
        return buildings_gdf

    @staticmethod
    async def get_scenario_context_buildings(
            scenario_id: int,
    ) -> gpd.GeoDataFrame:
        """
        Function retrieves scenario context buildings data from urban_api
        Args:
            scenario_id: scenario id to get buildings from
        Returns:
            gpd.GeoDataFrame: buildings layer
        Raises:
            404, http exception living buildings not found
        """

        context_buildings = await urban_api_handler.get(
            endpoint_url=f"/api/v1/scenarios/{scenario_id}/context/geometries_with_all_objects",
            params={
                "physical_object_id": 4,
            }
        )
        context_buildings_gdf = gpd.GeoDataFrame.from_features(context_buildings, crs=4326)
        if context_buildings_gdf.empty:
            raise http_exception(
                status_code=404,
                msg="No context buildings found",
                _input={
                    "scenario_id": scenario_id
                },
                _detail={
                    "living_buildings_data": context_buildings
                }
            )
        return context_buildings_gdf

    @staticmethod
    async def get_scenario_services(
            scenario_id: int,
            service_type_id: int,
    ) -> gpd.GeoDataFrame:
        """
        Function retrieves scenario services data from urban_api
        Args:
            scenario_id: scenario id to get services from
            service_type_id: service to get services from
        Returns:
            gpd.GeoDataFrame: services layer
        Raises:
            404, http exception services with service type not found
        """

        services = await urban_api_handler.get(
            endpoint_url=f"/api/v1/scenarios/{scenario_id}/geometries_with_all_objects",
            params={
                "service_type_id": service_type_id,
            }
        )
        services_gdf = gpd.GeoDataFrame.from_features(services, crs=4326)
        if services_gdf.empty:
            raise http_exception(
                status_code=404,
                msg="No services found",
                _input={
                    "scenario_id": scenario_id
                },
                _detail={
                    "services_data": services
                }
            )
        return services_gdf

    @staticmethod
    async def get_scenario_context_services(
            scenario_id: int,
            service_type_id: int,
    ) -> gpd.GeoDataFrame:
        """
        Function retrieves scenario context services data from urban_api
        Args:
            scenario_id: scenario id to get services from
            service_type_id: service to get services from
        Returns:
            gpd.GeoDataFrame: context services layer
        Raises:
            404, http exception context services with service type not found
        """

        context_services = await urban_api_handler.get(
            endpoint_url=f"/api/v1/scenarios/{scenario_id}/context/geometries_with_all_objects",
            params={
                "service_type_id": service_type_id,
            }
        )
        context_services_gdf = gpd.GeoDataFrame.from_features(context_services, crs=4326)
        if context_services_gdf.empty:
            raise http_exception(
                status_code=404,
                msg="No context services found",
                _input={
                    "scenario_id": scenario_id
                },
                _detail={
                    "context_services_data": context_services
                }
            )
        return context_services_gdf

    @staticmethod
    async def get_scenario_population_data(
            scenario_id: int
    ) -> int:
        """
        Function retrieves population data from urban_api
        Args:
            scenario_id: scenario id to get population data from
        Returns:
            gpd.GeoDataFrame: population data layer
        """

        population = await urban_api_handler.get(
            endpoint_url=f"/api/v1/scenarios/{scenario_id}/indicators_values",
            params={
                "indicators_ids": 1,
            }
        )

        return population[0]["value"]

    @staticmethod
    async def get_scenario_context_population_data(
        project_id: int
    ) -> int:
        """
        Function retrieves context population data from urban_api
        Args:
            project_id: project id to get population data from
        Returns:
            gpd.GeoDataFrame: context population data layer
        """

        project_data = await urban_api_handler.get(
            endpoint_url=f"/api/v1/projects/{project_id}",
        )
        context_territories = project_data["properties"]["context"]
        responses = [
            await urban_api_handler.get(
                endpoint_url=f"/api/v1/territory/{territory_id}/indicators_values",
            ) for territory_id in context_territories
            ]
        context_population = sum([territory_population[0]["value"] for territory_population in responses])
        return context_population


effects_api_gateway = EffectsAPIGateway()
