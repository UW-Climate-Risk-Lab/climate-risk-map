from dataclasses import dataclass

from typing import List, Optional

from config.settings import S3_BUCKET,FLOOD_AGENT_ALIAS_ID, FLOOD_AGENT_ID, WILDFIRE_AGENT_ALIAS_ID, WILDFIRE_AGENT_ID
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
    display_resolution: str # This will be in a format like 0p1deg (Meaning 0.1 degree resolution)
    unit: str
    min_value: float  # Min and max values used for colorbar
    max_value: float
    available_ssp: List[int]
    available_return_periods: Optional[List[int]]
    display_return_period: Optional[int] # Used for hazard overlay tiles in frontend
    available_year_periods: Optional[List[str]] # These should be in format like ["2015-2044", "2045-2074"]. Signifys the period of years that hazard was calculated for
    geotiff: Geotiff
    bedrock_agent_id: str
    bedrock_agent_alias_id: str

    def _validate_inputs(self, measure: str, ssp: int) -> bool:
        """Validate input parameters for geotiff URI generation."""
        if ssp not in self.available_ssp:
            logger.error(f"SSP{str(ssp)} is not available for {self.name}")
            return False
        
        if measure not in self.available_measures:
            logger.error(f"{measure} is not available for {self.name}")
            return False
        
        return True

    def _get_year_component(self, decade: int) -> str:
        """Get the appropriate year component for the filename."""
        if not self.available_year_periods:
            return str(decade)
        
        for year_period in self.available_year_periods:
            start_year = int(year_period[:4])
            end_year = int(year_period[5:9])
            if start_year <= decade < end_year:
                return year_period
            if decade == 2100:
                # Edge case to handle the last decade, which is most always 2100 for CMIP derived hazard overlays
                return self.available_year_periods[-1]
        
        # Fallback to decade if no period matches
        return str(decade)

    def _build_filename(self, measure: str, year_component: str, month: int) -> str:
        """Build the filename for the geotiff."""
        month_str = f"{month:02d}"
        
        if self.display_return_period:
            return f"{measure}-{year_component}-{month_str}-{self.display_return_period}-global-{self.display_resolution}.tif"
        else:
            return f"{measure}-{year_component}-{month_str}-global-{self.display_resolution}.tif"

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
            region (Region): Geographic region
            return_period (int, optional): Return period for flood analysis

        Returns:
            str: S3 URI to the geotiff, or None if validation fails
        """
        # Validate inputs
        if not self._validate_inputs(measure, ssp):
            return None
        
        # Get year component and build filename
        year_component = self._get_year_component(decade)
        filename = self._build_filename(measure, year_component, month)
        
        # Construct and return S3 URI
        return f"s3://{self.geotiff.s3_bucket}/{self.geotiff.s3_prefix}/ssp{ssp}/{self.geotiff.format}/global/{filename}"


class HazardConfig:

    HAZARDS = [
        Hazard(
            name="wildfire",
            label=r"Wildfire Risk Index",
            available_measures=[
                "ensemble_mean",
                "ensemble_median",
                "ensemble_max",
                "ensemble_min",
                "ensemble_stdev",
                "ensemble_q1",
                "ensemble_q3",
                "usda_burn_probability",
                "ensemble_mean_historic_baseline"
            ],
            display_measure="ensemble_q3",
            display_resolution="0p05deg",
            unit="",
            min_value=0,
            max_value=20,
            available_ssp=[245, 585],
            available_return_periods=None,
            available_year_periods=None,
            display_return_period=None,
            geotiff=Geotiff(
                format="cogs",
                s3_bucket=S3_BUCKET,
                s3_prefix="climate-risk-map/frontend/NEX-GDDP-CMIP6/fwi",
                colormap="oranges",
                opacity=0.6,
            ),
            bedrock_agent_id=WILDFIRE_AGENT_ID,
            bedrock_agent_alias_id=WILDFIRE_AGENT_ALIAS_ID
        ),
        Hazard(
            name="flood",
            label=r"Pluvial Flood Risk Index",
            available_measures=[
                "ensemble_median",
                "ensemble_q3",
                "ensemble_median_historic_baseline",
                "ensemble_q3_historic_baseline",
                "flood_zone",
                "is_sfha",
                "flood_zone_subtype",
                "flood_depth",
            ],
            display_measure="ensemble_q3",
            display_resolution="0p05deg",
            unit="",
            min_value=0,
            max_value=100,
            available_ssp=[245, 585],
            available_return_periods=[2, 5, 20, 100, 500],
            display_return_period=100,
            available_year_periods=["2015-2044", "2045-2074", "2075-2100"],
            geotiff=Geotiff(
                format="cogs",
                s3_bucket=S3_BUCKET,
                s3_prefix="climate-risk-map/frontend/NEX-GDDP-CMIP6/future_monthly_pfe_mm_day_decade_month",
                colormap="blues",
                opacity=0.6,
            ),
            bedrock_agent_id=FLOOD_AGENT_ID,
            bedrock_agent_alias_id=FLOOD_AGENT_ALIAS_ID
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
