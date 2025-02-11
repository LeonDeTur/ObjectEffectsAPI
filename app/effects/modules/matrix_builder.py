import pandas as pd
import geopandas as gpd


class MatrixBuilder:

    @staticmethod
    async def availability_matrix(
            buildings: gpd.GeoDataFrame,
            services: gpd.GeoDataFrame,
    ) -> pd.DataFrame:
        """
        Calculated availability matrix with walk simulation
        Args:
            buildings (gpd.GeoDataFrame): Building geometries
            services (gpd.GeoDataFrame): Service geometries
        Returns:
            pd.DataFrame: Availability matrix with distance in minutes
        """

        buildings.index = buildings.building_id
