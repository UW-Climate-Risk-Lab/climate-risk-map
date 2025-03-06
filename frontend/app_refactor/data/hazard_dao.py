import logging

import httpx
import json

from config.settings import TITILER_ENDPOINT
from config.map_config import MapConfig
from config.hazard_config import Hazard
from config.map_config import Region

logger = logging.getLogger(__name__)

def query_titiler(endpoint: str, params):
    try:
        r = httpx.get(url=endpoint, params=params)
        
        if r.content:
            try:
                detail = json.loads(r.content.decode('utf-8'))["detail"]
            except json.JSONDecodeError:
                logger.warning("Invalid JSON format in TiTiler content bytes data.")
                detail = ""
            except UnicodeDecodeError:
                logger.warning("Unable to decode TiTiler content bytes data with utf-8.")
                detail = ""
            except Exception as e:
                logger.warning("Unable to access TiTiler response detail")
                detail = ""
        else:
            detail = ""
        
        r.raise_for_status()  # Raise an exception for HTTP errors
    
    except httpx.TimeoutException as e:
        logger.error(f"TiTiler Timeout Error: {str(e)}\nURL: {params['url']}")
        # Return a specific response for timeout
        return {"error": "timeout", "message": "Service timed out, please try again"}

    except httpx.RequestError as e:
        logger.error(f"TiTiler Request Error: {str(e)}\nTiTiler Detail: {detail}\nURL: {params["url"]}")
    
    except httpx.HTTPStatusError as e:
        logger.error(f"TiTiler Status Error: {str(e)}\nTiTiler Detail: {detail}\nURL: {params["url"]}")
    
    try:
        return r.json()
    except ValueError as e:
        logger.error(f"TiTiler Value Error: {str(e)}\nTiTiler Detail: {detail}\nURL: {params["url"]}")

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
    ) -> str | None:
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
        try:
            tiles = r["tiles"][0]
        except Exception as e:
            tiles = None
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