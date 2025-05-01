from dataclasses import dataclass

from typing import List

from config.settings import S3_BUCKET
from config.map_config import Region

import logging

logger = logging.getLogger(__name__)


@dataclass
class Geotiff:
    format: str  # commonly "cogs" for cloud optimized geotiff]
    s3_bucket: str
    s3_prefix: str
    colormap: str
    opacity: float


@dataclass
class Hazard:
    name: str
    label: str
    available_measures: List[str]
    display_measure: str
    unit: str
    min_value: float  # Min and max values used for colorbar
    max_value: float
    available_ssp: List[int]
    geotiff: Geotiff

    def get_hazard_geotiff_s3_uri(
        self, measure: str, ssp: int, decade: int, month: int, region: Region
    ) -> str:
        """Geotiffs live in S3 and are segmented by time step (for now decade and month),
        aggregation measure, and SSP scenario. This method takes a hazard and the given inputs
        and produces the S3 URI to the geotiff

        Args:
            measure (str): Aggregation measure (example: ensemble_mean)
            ssp (int): SSP scenario
            decade (int): Decade
            month (int): Month

        Returns:
            str: S3 URI
        """

        if ssp not in self.available_ssp:
            logger.error(f"SSP{str(ssp)} is not available for {self.name}")
            return None
        if measure not in self.available_measures:
            logger.error(f"{measure} is not available for {self.name}")
            return None
        file = f"{measure}-{decade}-{month:02d}-{region.dbname}.tif"
        uri = f"s3://{self.geotiff.s3_bucket}/{self.geotiff.s3_prefix}/ssp{str(ssp)}/{self.geotiff.format}/{region.dbname}/{file}"
        return uri


class HazardConfig:

    HAZARDS = [
        Hazard(
            name="fwi",
            label=r"Fire Weather Index (NASA NEX GDDP Ensemble Mean)",
            available_measures=[
                "ensemble_mean",
                "ensemble_median",
                "ensemble_max",
                "ensemble_min",
                "ensemble_stdev",
                "ensemble_q1",
                "ensemble_q3",
            ],
            display_measure="ensemble_mean",
            unit="",
            min_value=0,
            max_value=25,
            available_ssp=[245, 585],
            geotiff=Geotiff(
                format="cogs",
                s3_bucket=S3_BUCKET,
                s3_prefix="climate-risk-map/frontend/NEX-GDDP-CMIP6/fwi",
                colormap="oranges",
                opacity=0.6,
            ),
        )
    ]

    @classmethod
    def get_hazard(cls, hazard_name: str) -> Hazard:
        """Returns hazard object

        Args:
            hazard_name (str): Name of hazard indicator
        """
        for hazard in cls.HAZARDS:
            if hazard.name == hazard_name:
                return hazard
        logger.error(f"Hazard '{hazard_name}' that was requested is not configured")
        return None
