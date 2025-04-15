import logging
import json

from typing import List, Dict
from geojson_pydantic import FeatureCollection

from config.map_config import Region
from config.exposure.asset import Asset, OpenStreetMapAsset, HifldAsset
from config.hazard_config import Hazard

from dao.database import DatabaseManager

from api.v1.app.schemas import GetDataInputParameters
from api.v1.app.query import GetDataQueryBuilder

logger = logging.getLogger(__name__)


class ExposureDAO:
    """Data Access Object for asset exposure data"""

    @staticmethod
    def get_exposure_data(
        region: Region,
        assets: List[Asset],
        hazard: Hazard = None,
        ssp: int = None,
        month: List[int] = None,
        decade: List[int] = None,
        bbox=None,
    ) -> Dict:
        """Get data for download or display. This returns our exposed assets of interest,
        and corresponding hazard exposure if cliamte params passed in

        Args:
            region (Region): Region of interest to get data from
            assets (List[Asset]): Assets of interest
            hazard (Hazard, optional): Hazard of asset exposures. Defaults to None.
            ssp (int, optional): SSP scenario. Defaults to None.
            month (List[int], optional): Month(s) to get exposure for. Defaults to None.
            decade (List[int], optional): Decade(s) to get exposure for. Defaults to None.
            bbox (_type_, optional): Geospatial Bounding Box to get assets from. Defaults to None.

        Returns:
            Dict: GeoJSON spec dictionary
        """
        logger.debug(f"Preparing download data for region={region.name}")

        if len(set([type(asset) for asset in assets])) > 1:
            logger.error("All assets must come from same source")
            return {"type": "FeatureCollection", "features": []}

        if all([isinstance(asset, OpenStreetMapAsset) for asset in assets]):
            data = ExposureDAO.get_osm_exposure_data(
                region=region,
                assets=assets,
                hazard=hazard,
                ssp=ssp,
                month=month,
                decade=decade,
                bbox=bbox,
            )
            return data

        if all([isinstance(asset, HifldAsset) for asset in assets]):
            data = ExposureDAO.get_hifld_exposure_data(region=region, assets=assets)
            return data

    @staticmethod
    def get_hifld_exposure_data(region: Region, assets: List[HifldAsset]) -> Dict:
        """Load and combine multiple HIFLD GeoJSON files into a single GeoJSON dictionary.

        Args:
            region (Region): Region of interest (currently unused but kept for API consistency)
            assets (List[HifldAsset]): List of HIFLD assets to load

        Returns:
            Dict: Combined GeoJSON containing features from all asset files
        """

        combined_geojson = {"type": "FeatureCollection", "features": []}

        for asset in assets:
            try:
                with open(asset.geojson_path, "r") as f:
                    geojson = json.load(f)
                    if "features" in geojson:
                        # Add asset name as a property to each feature
                        for feature in geojson["features"]:
                            if "properties" not in feature:
                                feature["properties"] = {}
                            feature["properties"]["asset_name"] = asset.name

                        combined_geojson["features"].extend(geojson["features"])
                        logger.debug(
                            f"Loaded {len(geojson['features'])} features from {asset.name}"
                        )
                    else:
                        logger.warning(f"No features found in {asset.geojson_path}")

            except Exception as e:
                logger.error(f"Error loading {asset.geojson_path}: {str(e)}")
                continue

        return combined_geojson

    @staticmethod
    def get_osm_exposure_data(
        region: Region,
        assets: List[OpenStreetMapAsset],
        hazard: Hazard = None,
        ssp: int = None,
        month: List[int] = None,
        decade: List[int] = None,
        bbox=None,
    ) -> Dict:
        

        categories = set([asset.osm_category for asset in assets])

        # If there is a bounding box provided, we convert to a standard GeoJSON pydantic object
        if bbox:
                bbox = FeatureCollection(features=bbox["features"], type="FeatureCollection")
        try:
            climate_variable = hazard.name
        except Exception as e:
            climate_variable = None
        all_data = {"type": "FeatureCollection", "features": []}

        # We loop here since database query is designed to only get data from a single category
        for category in categories:
            osm_types = [asset.osm_type for asset in assets if asset.osm_category == category]
            osm_subtypes = [asset.osm_subtype for asset in assets if asset.osm_subtype and asset.osm_category == category]

            if len(osm_subtypes) == 0:
                osm_subtypes = None

            try:
                input_params = GetDataInputParameters(
                    osm_category=category,
                    osm_types=osm_types,
                    osm_subtypes=osm_subtypes,
                    bbox=bbox,
                    county=True,
                    city=True,
                    epsg_code=4326,
                    climate_variable=climate_variable,
                    climate_ssp=ssp,
                    climate_month=month,
                    climate_decade=decade,
                    climate_metadata=False,
                )
                query, query_params = GetDataQueryBuilder(input_params).build_query()

                data = DatabaseManager.execute_query(dbname=region.dbname, query=query, params=query_params)
                data = data[0][0]
                all_data["features"].extend(data["features"])
            except Exception as e:
                logger.error(f"Error fetching download data: {str(e)}")
                data = {"type": "FeatureCollection", "features": []}

        return all_data
