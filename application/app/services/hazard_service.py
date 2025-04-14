import logging

from typing import Tuple, List

from dao.hazard_dao import HazardRasterDAO
from config.hazard_config import HazardConfig
from config.map_config import MapConfig

logger = logging.getLogger(__name__)


class HazardService:
    """Service for handling hazard/climate related operations"""

    @staticmethod
    def get_hazard_tilejson_url(
        hazard_name: str,
        ssp: int,
        month: int,
        decade: int,
        region_name: str,
    ) -> Tuple[str, float]:
        """Processes request to get url of hazard tile overlay

        Args:
            hazard_name (str): Name of hazard
            ssp (int): SSP sceanrio
            month (int): Month
            decade (int): Decade
            region_name (str): Name of the region selected

        Returns:
            str, float: URL to titiler tilejson and the opactity level of the tiles
        """

        hazard = HazardConfig.get_hazard(hazard_name=hazard_name)
        region = MapConfig.get_region(region_name=region_name)

        if not hazard:
            # We return the original placeholder overlay url to serve map tiles
            return (
                MapConfig.BASE_MAP_COMPONENT["hazard_tile_layer"]["placeholder_url"],
                MapConfig.BASE_MAP_COMPONENT["hazard_tile_layer"][
                    "placeholder_opacity"
                ],
            )

        if not region:
            # We return the original placeholder overlay url to serve map tiles
            return (
                MapConfig.BASE_MAP_COMPONENT["hazard_tile_layer"]["placeholder_url"],
                MapConfig.BASE_MAP_COMPONENT["hazard_tile_layer"][
                    "placeholder_opacity"
                ],
            )

        hazard_tilejson_url = HazardRasterDAO.get_hazard_tilejson_url(
            hazard=hazard,
            decade=decade,
            month=month,
            ssp=ssp,
            region=region,
            measure=hazard.display_measure,
        )

        if not hazard_tilejson_url:
            return (
                MapConfig.BASE_MAP_COMPONENT["hazard_tile_layer"]["placeholder_url"],
                MapConfig.BASE_MAP_COMPONENT["hazard_tile_layer"][
                    "placeholder_opacity"
                ],
            )

        return hazard_tilejson_url, hazard.geotiff.opacity

    @staticmethod
    def get_available_ssp(hazard_name: str) -> List[str]:

        hazard = HazardConfig.get_hazard(hazard_name=hazard_name)

        if not hazard:
            return list()

        return [str(ssp) for ssp in hazard.available_ssp]
