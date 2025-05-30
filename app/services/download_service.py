"""
This module contains a service layer for handling user data downloads.
It provides functionality for validating download requests, enforcing download limits,
and processing geographic data for export.
"""

import logging
import dash_leaflet as dl
import pandas as pd
import ast
from collections import defaultdict
from numpy import nan

from typing import List, Dict
from dataclasses import dataclass


from config.settings import MAX_DOWNLOADS, MAX_DOWNLOAD_AREA
from config.hazard_config import HazardConfig, Hazard
from config.exposure import Asset, get_asset
from config.map_config import MapConfig, Region
from dao.exposure_dao import ExposureDAO
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
        
        assets = [get_asset(name=asset_label) for asset_label in asset_overlays]

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
                "To download data, please select all dropdowns!"
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
                "Your selected area is too large to download"
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
        
        assets = [get_asset(name=asset_label) for asset_label in asset_overlays]

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

    @staticmethod
    def parse_osm_tags(df, tag_format='prefix', drop_original_tags=True):
        """
        Advanced function to parse OSM tags with multiple formatting options.
        
        Parameters:
        - df: A pandas DataFrame with a 'tags' column containing Python dict-like strings
        - tag_format: How to format the tag columns. Options:
            - 'prefix': Prefix columns with the osm_subtype (e.g., "line;voltage")
            - 'separate': Return a dictionary of DataFrames, one for each osm_subtype
            - 'flat': Don't prefix columns, just use tag keys as column names
        - drop_original_tags: Whether to drop the original 'tags' column
        
        Returns:
        - Depending on tag_format:
            - For 'prefix', 'hierarchical', and 'flat': A DataFrame with tags as columns
            - For 'multiindex': A DataFrame with multi-index columns
            - For 'separate': A dictionary of DataFrames, keyed by osm_subtype
        """
        # Make a copy to avoid modifying the original
        result_df = df.copy()
        
        # Early exit if no tags column
        if 'tags' not in result_df.columns:
            return result_df
        
        # Define a function to safely parse the Python-style dict strings
        def safe_parse_tags(tag_str):
            if isinstance(tag_str, dict):
                return tag_str
            if pd.isna(tag_str) or not isinstance(tag_str, str):
                return {}
            try:
                # Using ast.literal_eval which is safe for parsing Python literals
                return ast.literal_eval(tag_str)
            except (SyntaxError, ValueError):
                try:
                    # Try basic cleanup
                    cleaned = tag_str.replace("'", '"').replace('None', 'null')
                    import json
                    return json.loads(cleaned)
                except Exception as e:
                    print(f"Warning: Could not parse tag string: {tag_str}\n{str(e)}")
                    return {}
        
        # Parse all tags
        tag_dicts = [safe_parse_tags(tag) for tag in result_df['tags']]
        
        # Handle each format
        if tag_format == 'separate':
            # Create a dictionary to store DataFrames for each subtype
            subtype_dfs = {}
            
            # Check if osm_subtype exists
            if 'osm_subtype' not in result_df.columns:
                raise ValueError("Cannot use 'separate' format without 'osm_subtype' column.")
            
            # Group by osm_subtype
            for subtype, group in result_df.groupby('osm_subtype'):
                if pd.isna(subtype) or not subtype:
                    continue
                    
                # Create a new DataFrame for this subtype
                subtype_df = group.copy()
                if drop_original_tags:
                    subtype_df.drop('tags', axis=1, inplace=True)
                
                # Dictionary to store column data
                column_data = defaultdict(lambda: [nan] * len(subtype_df))
                
                # Process each row in this group
                for i, idx in enumerate(subtype_df.index):
                    orig_i = df.index.get_loc(idx)
                    tags = tag_dicts[orig_i]
                    
                    if not tags:
                        continue
                        
                    # Process each tag
                    for key, value in tags.items():
                        column_data[key][i] = value
                
                # Add columns to the DataFrame
                for key, values in column_data.items():
                    subtype_df[key] = values
                
                # Store the DataFrame
                subtype_dfs[subtype] = subtype_df
            
            return subtype_dfs
            
        else:  # 'prefix' or 'flat'
            # Dictionary to store column data
            column_data = defaultdict(lambda: [nan] * len(result_df))
            
            # For prefixing with subtype
            if tag_format == 'prefix' and 'osm_subtype' in result_df.columns:
                subtypes = result_df['osm_subtype'].tolist()
                
                # Process each row
                for i, (tags, subtype) in enumerate(zip(tag_dicts, subtypes)):
                    if pd.isna(subtype) or not subtype or not tags:
                        continue
                        
                    # Process each tag
                    for key, value in tags.items():
                        col_name = f"{subtype};{key}"
                        column_data[col_name][i] = value
            else:
                # Without prefixing
                for i, tags in enumerate(tag_dicts):
                    if not tags:
                        continue
                        
                    # Process each tag
                    for key, value in tags.items():
                        column_data[key][i] = value
            
            # Add columns to the result DataFrame
            for col_name, values in column_data.items():
                result_df[col_name] = values
        
        # Drop the original tags column if requested
        if drop_original_tags:
            result_df.drop('tags', axis=1, inplace=True)
        
        return result_df


