"""
This module contains a serice layer for user downloading data

"""

import logging
import dash_leaflet as dl
import pandas as pd

from dash import dcc

from typing import List, Dict
from dataclasses import dataclass


from config.settings import MAX_DOWNLOADS, MAX_DOWNLOAD_AREA
from config.hazard_config import HazardConfig, Hazard
from config.asset_config import AssetConfig, Asset
from config.map_config import MapConfig, Region
from data.exposure_dao import ExposureDAO
from utils.geo_utils import calc_bbox_area, geojson_to_pandas

from config.map_config import MapConfig, Region

logger = logging.getLogger(__name__)

dcc.send_data_frame

@dataclass
class DownloadConfig:
    data_sender: Dict | None # This will be a 
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
    def _check_download_criteria(download_config: DownloadConfig) -> DownloadConfig:

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
        
        if None in [download_config.hazard, download_config.ssp, download_config.month, download_config.decade, download_config.region]:
            download_config.download_message = (
                f"To download data, please select all dropdowns!"
            )
            download_config.download_message_is_open = True
            download_config.download_message_color = "danger"
            return download_config

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
        download_count: int,
    ):
        
        logger.info(f"Download requested: region={region_name}, hazard={hazard_name}, ssp={ssp}, month={month}, decade={decade}")

        hazard = HazardConfig.get_hazard(hazard_name=hazard_name)
        
        # Here, we use the passed in Overlay components to extract the Asset type, if the layer is checked by the user
        # The overlay id is set as the asset name in MapService.get_asset_overlays.
        # Technically the overlays contain the geojson of asset data already, but we requery the database to get the climate exposure
        # In the future, we can perhaps optimize this to not requery the features, but for now the extra query is not a high cost
        assets = [AssetConfig.get_asset(name=asset["props"]["id"]) for asset in asset_overlays if asset["props"]["checked"]]

        region = MapConfig.get_region(region_name=region_name)

        if download_count is None:
            download_count = 0

        download = DownloadConfig(
            data_sender=None,
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

        download = DownloadService._check_download_criteria(download_config=download)

        if download.download_allowed:
            geojson = ExposureDAO.get_exposure_data(
                hazard=download.hazard,
                region=download.region,
                assets=download.assets,
                bbox=download.shapes,
                ssp=ssp,
                month=[month],
                decade=[decade]
            )
            df = geojson_to_pandas(data=geojson)

            if len(df) == 0:
                download.data_sender = None
                download.download_message = "No data was found over the selected area"
                download.download_message_is_open = True
                download.download_message_color = "warning"
            else:
                download.data_sender = dcc.send_data_frame(df.to_csv, "climate_risk_map_download.csv")
                download.download_count += 1
                download.download_message = "Download is in progress!"
                download.download_message_is_open = True
                download.download_message_color = "success"
                logger.info(f"Download completed: {len(df)} records, region={region_name}")
            return download

        else:
            return download



