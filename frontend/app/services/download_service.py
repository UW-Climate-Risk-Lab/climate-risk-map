"""
This module contains a service layer for handling user data downloads.
It provides functionality for validating download requests, enforcing download limits,
and processing geographic data for export.
"""

import logging
import dash_leaflet as dl
import pandas as pd

from typing import List, Dict
from dataclasses import dataclass


from config.settings import MAX_DOWNLOADS, MAX_DOWNLOAD_AREA
from config.hazard_config import HazardConfig, Hazard
from config.asset.config import AssetConfig, Asset
from config.map_config import MapConfig, Region
from data.exposure_dao import ExposureDAO
from utils.geo_utils import calc_bbox_area, geojson_to_pandas

logger = logging.getLogger(__name__)

@dataclass
class DownloadConfig:
    hazard: Hazard | None
    assets: List[Asset]
    region: Region
    ssp: int
    decade: int
    month: int
    shapes: Dict
    n_clicks: int | None
    download_count: int | None
    download_message: str
    download_message_is_open: bool
    download_message_color: str | None
    download_allowed: bool


class DownloadService:

    @staticmethod
    def create_download_config(shapes: Dict,
        asset_overlays: List[dl.Overlay],
        region_name: str,
        hazard_name: str,
        ssp: int,
        decade: int,
        month: int,
        download_count: int,) -> DownloadConfig:
        """
        Creates and validates a download configuration based on user selections.

        Args:
            shapes (Dict): GeoJSON-like dictionary containing selected geographic features
            asset_overlays (List[dl.Overlay]): List of selected asset overlay layers
            region_name (str): Name of the selected geographic region
            hazard_name (str): Name of the selected climate hazard
            ssp (int): Selected Shared Socioeconomic Pathway scenario number
            decade (int): Target decade for climate projections
            month (int): Selected month for temporal filtering
            download_count (int): Current number of downloads in the session

        Returns:
            DownloadConfig: Configuration object containing validated download parameters
                          and status messages
        """
        logger.info(f"Download requested: region={region_name}, hazard={hazard_name}, ssp={ssp}, month={month}, decade={decade}")

        hazard = HazardConfig.get_hazard(hazard_name=hazard_name)
        
        assets = [AssetConfig.get_asset(name=asset_label) for asset_label in asset_overlays]

        region = MapConfig.get_region(region_name=region_name)

        if download_count is None:
            download_count = 0

        download_config = DownloadConfig(
            hazard=hazard,
            assets=assets,
            region=region,
            ssp=ssp,
            month=month,
            decade=decade,
            shapes=shapes,
            n_clicks=0,
            download_count=download_count,
            download_message="",
            download_message_is_open=False,
            download_message_color=None,
            download_allowed=False,
        )

        download = DownloadService._check_download_criteria(download_config=download_config)

        return download


    @staticmethod
    def _check_download_criteria(download_config: DownloadConfig) -> DownloadConfig:
        """
        Validates a download request against system constraints and user limits.

        Checks for:
        - Completeness of required parameters
        - Region availability for downloads
        - Presence of selected map areas
        - Download count limits
        - Maximum area constraints

        Args:
            download_config (DownloadConfig): The download configuration to validate

        Returns:
            DownloadConfig: Updated configuration with validation results and status messages
        """
        if None in [download_config.region, download_config.hazard, download_config.ssp, download_config.month, download_config.decade, download_config.region]:
            download_config.download_message = (
                f"To download data, please select all dropdowns!"
            )
            download_config.download_message_is_open = True
            download_config.download_message_color = "danger"
            return download_config

        if not download_config.region.available_download:
            download_config.download_message = f"The region `{download_config.region.label}` is not yet available for download"
            download_config.download_message_is_open = True
            download_config.download_message_color = "warning"
            return download_config

        if (
            download_config.shapes is None
            or len(download_config.shapes["features"]) == 0
        ):
            download_config.download_message = "Please select an area on the map (Hint: Click the black square in the upper right of the map)."
            download_config.download_message_is_open = True
            download_config.download_message_color = "warning"
            return download_config

        if download_config.download_count >= MAX_DOWNLOADS:
            download_config.download_message = f"You have reached the maximum of {MAX_DOWNLOADS} downloads per session."
            download_config.download_message_is_open = True
            download_config.download_message_color = "danger"
            return download_config

        if (
            calc_bbox_area(features=download_config.shapes["features"])
            > MAX_DOWNLOAD_AREA
        ):
            download_config.download_message = (
                f"Your selected area is too large to download"
            )
            download_config.download_message_is_open = True
            download_config.download_message_color = "danger"
            return download_config

        download_config.download_message = "Download is in progress!"
        download_config.download_message_is_open = True
        download_config.download_message_color = "success"
        download_config.download_allowed = True

        return download_config

    @staticmethod
    def get_download(
        shapes: Dict,
        asset_overlays: List[dl.Overlay],
        region_name: str,
        hazard_name: str,
        ssp: int,
        decade: int,
        month: int,
    ) -> pd.DataFrame:
        """
        Retrieves and processes exposure data for download based on user selections.

        Args:
            shapes (Dict): GeoJSON-like dictionary containing selected geographic features
            asset_overlays (List[dl.Overlay]): List of selected asset overlay layers
            region_name (str): Name of the selected geographic region
            hazard_name (str): Name of the selected climate hazard
            ssp (int): Selected Shared Socioeconomic Pathway scenario number
            decade (int): Target decade for climate projections
            month (int): Selected month for temporal filtering

        Returns:
            pd.DataFrame: A pandas DataFrame containing the processed exposure data
                        ready for download
        """
        hazard = HazardConfig.get_hazard(hazard_name=hazard_name)
        
        assets = [AssetConfig.get_asset(name=asset_label) for asset_label in asset_overlays]

        region = MapConfig.get_region(region_name=region_name)

        geojson = ExposureDAO.get_exposure_data(
                hazard=hazard,
                region=region,
                assets=assets,
                bbox=shapes,
                ssp=ssp,
                month=[month],
                decade=[decade]
        )
        
        df = geojson_to_pandas(data=geojson)

        return df



