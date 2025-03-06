import logging

from typing import List, Dict

from config.map_config import Region
from config.asset_config import Asset
from config.hazard_config import Hazard

from data.database import DatabaseManager
from data.api import infraXclimateAPI, infraXclimateInput

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
        
        categories = set()
        osm_types = []
        osm_subtypes = []

        for asset in assets:
            osm_types = osm_types + asset.osm_types
            osm_subtypes = osm_subtypes + asset.osm_subtypes
            categories.add(asset.osm_category)
        
        if len(categories) != 1:
            logger.error("Exactly one OSM category is required when querying exposure data")
            return {"type": "FeatureCollection", "features": []}
        else:
            category = list(categories)[0]
        
        try:
            climate_variable = hazard.name
        except Exception as e:
            climate_variable = None

        with DatabaseManager.get_connection(region.dbname) as conn:
            try:
                params = infraXclimateInput(
                    category=category,
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
                
                api = infraXclimateAPI(conn=conn)
                data = api.get_data(input_params=params)
                del api
            except Exception as e:
                logger.error(f"Error fetching download data: {str(e)}")
                data = {"type": "FeatureCollection", "features": []}
        return data