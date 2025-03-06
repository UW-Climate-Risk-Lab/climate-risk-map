import logging

import httpx

from config.settings import TITILER_ENDPOINT
from config.map_config import MapConfig
from config.hazard_config import Hazard
from config.map_config import Region

logger = logging.getLogger(__name__)

def query_titiler(endpoint: str, params):
    try:
        r = httpx.get(url=endpoint, params=params)
        r.raise_for_status()  # Raise an exception for HTTP errors

    except httpx.RequestError as e:
        raise ConnectionError("Unable to connect to Titiler Endpoint!") from e
    
    except httpx.HTTPStatusError as e:
        raise ConnectionError(f"Error response {e.response.status_code} from Titiler Endpoint!") from e
    
    try:
        return r.json()
    except ValueError as e:
        raise ValueError("Invalid JSON response from Titiler Endpoint!") from e

class HazardRasterDAO:
    """
    Data Access Object for climate hazard data stored as rasters (generally Cloud Optimized GeoTiffs)
    We make use of TiTiler, an open-source raster tiling service.
    """
    
    @staticmethod
    def get_hazard_tilejson_url(
        hazard: Hazard,
        measure: str,
        decade: int,
        month: int,
        ssp: int,
        region: Region
    ):
        """For requested hazard indicator, queries TiTiler for specific geotiff
        and returns tiles

        Args:
            hazard (Hazard): Hazard object describing attributes of hazard data
            measure (str): Measure of the hazard (example: ensemble_mean)
            decade (int): Decade
            month (int): Month
            ssp (int): SSP

        Returns:
            _type_: _description_
        """

        endpoint = f"{TITILER_ENDPOINT}/cog/WebMercatorQuad/tilejson.json"
        file_uri = hazard.get_hazard_geotiff_s3_uri(measure=measure, ssp=ssp, decade=decade, month=month, region=region)
        if not file_uri:
            return None
        params = {
            "url": file_uri,
            "rescale": f"{hazard.min_value},{hazard.max_value}",
            "colormap_name": hazard.geotiff.colormap,
        }
        r = query_titiler(endpoint=endpoint, params=params)
        tiles = r["tiles"][0]
        return tiles

    
    @staticmethod
    def get_climate_metadata(region, climate_variable=None):
        """Get metadata about available climate variables for a state
        
        Args:
            state (str): State name
            climate_variable (str, optional): Specific climate variable to get metadata for
            
        Returns:
            dict: Climate metadata
        """
        pass