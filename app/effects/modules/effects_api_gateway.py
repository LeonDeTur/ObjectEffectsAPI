import pandas as pd


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
