import logging

from typing import List

from config.map_config import Region
from config.asset_config import Asset

from data.database import DatabaseManager
from data.api import infraXclimateAPI, infraXclimateInput

logger = logging.getLogger(__name__)

class ExposureDAO:
    """Data Access Object for asset exposure data"""
    
    @staticmethod
    def get_exposure_data(
        region: Region, 
        assets: List[Asset], 
        bbox=None, 
        climate_params=None
    ):
        """Get data for download or display. This returns our exposed assets of interest,
        and corresponding hazard exposure if cliamte params passed in
        
        Args:
            region (Region): Region of interest for exposure data
            asset (List[Asset]); Asset(s) of interest for exposure data
            bbox (dict, optional): Bounding box for filtering. Defaults to None.
            climate_params (dict, optional): Climate data parameters. Defaults to None.
                Should contain: climate_variable, climate_ssp, climate_month, climate_decade
                
        Returns:
            dict: GeoJSON data with climate information if requested
        """
        logger.debug(f"Preparing download data for region={region.name}")
        
        # Set defaults for climate params
        climate_variable = None
        climate_ssp = None
        climate_month = None
        climate_decade = None
        
        # Extract climate parameters if provided
        if climate_params:
            climate_variable = climate_params.get("climate_variable")
            climate_ssp = climate_params.get("climate_ssp")
            climate_month = climate_params.get("climate_month")
            climate_decade = climate_params.get("climate_decade")
        

        categories = set()
        osm_types = []
        osm_subtypes = []

        for asset in assets:
            osm_types = osm_types + asset.osm_types
            osm_subtypes = osm_subtypes + asset.osm_subtypes
            categories.add(asset.osm_category)
        
        if len(category) != 1:
            logger.error("Exactly one OSM category is required when querying exposure data")
            return {"type": "FeatureCollection", "features": []}
        else:
            category = list(categories)[0]
        

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
                    climate_ssp=climate_ssp,
                    climate_month=climate_month,
                    climate_decade=climate_decade,
                    climate_metadata=False,
                )
                
                api = infraXclimateAPI(conn=conn)
                data = api.get_data(input_params=params)
                return data
            except Exception as e:
                logger.error(f"Error fetching download data: {str(e)}")
                return {"type": "FeatureCollection", "features": []}