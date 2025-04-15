from typing import Dict, List
import logging
import json
import os

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from psycopg2 import sql

from . import database

from . import config
from . import schemas
from . import utils

from .query import GetDataQueryBuilder

router = APIRouter(prefix="/data")  # Add prefix to ensure all API routes are under /data

S3_BUCKET = str(os.environ["S3_BUCKET"])
S3_PREFIX_USER_DOWNLOADS = str(os.environ["S3_BASE_PREFIX_USER_DOWNLOADS"])
DATA_SIZE_RETURN_LIMIT_MB=float(os.environ["DATA_SIZE_RETURN_LIMIT_MB"])

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@router.get("/{format}/{osm_category}/{osm_type}/")  # Was: "/data/{format}/{osm_category}/{osm_type}/"
def get_data(
    format: str,  # TODO: configure to allow CSV or Geojson
    osm_category: str,
    osm_type: str,
    osm_subtype: List[str] | None = Query(None),
    bbox: List[str] | None = Query(None),
    epsg_code: int = 4326,
    geom_type: str | None = None,
    climate_variable: str | None = None,
    climate_ssp: int | None = None,
    climate_month: int | None = None,
    climate_decade: int | None = None,
    limit: int | None = None,
) -> Dict:

    # Update format check first
    if format.lower() not in ["geojson", "csv"]:
        raise HTTPException(
            status_code=422, detail=f"{format} response format not supported"
        )

    # Convert input parameters
    osm_types = (osm_type,)
    if climate_month:
        climate_month = (climate_month,)
    if climate_decade:
        climate_decade = (climate_decade,)
    if osm_subtype:
        osm_subtype = tuple(osm_subtype)

    # Process bbox if provided
    if bbox:
        try:
            bbox_list = [schemas.BoundingBox(**json.loads(box)) for box in bbox]
            bbox = utils.create_bbox(bbox_list)
        except json.JSONDecodeError:
            input_format = '{"xmin": -126.0, "xmax": -119.0, "ymin": 46.1, "ymax": 47.2}'
            return {"error": f"Invalid bounding box JSON format. Example: bbox={input_format}"}
        except Exception as e:
            logger.error(f"Error creating geojson from bounding box input: {str(e)}")
            raise HTTPException(status_code=500, detail="Error parsing bounding boxes")

    try:
        # Validate input parameters using schema
        input_params = schemas.GetDataInputParameters(
            osm_category=osm_category,
            osm_types=osm_types,
            osm_subtypes=osm_subtype,
            bbox=bbox,
            epsg_code=epsg_code,
            geom_type=geom_type,
            climate_variable=climate_variable,
            climate_ssp=climate_ssp,
            climate_month=climate_month,
            climate_decade=climate_decade,
            limit=limit,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    # Continue with query if not cached
    query, query_params = GetDataQueryBuilder(input_params).build_query()

    result = database.execute_query(query=query, params=query_params)
    result = result[0][0]

    try:
        if result["features"] is None:
            result["features"] = list()
        else:
            result = utils.clean_geojson_data(raw_geojson=result)
    except KeyError as e:
        logger.error("Get GeoJSON database response has no key 'features'")

    try:
        schemas.GetGeoJsonOutput(geojson=result)
    except Exception as e:
        logger.error(
            f"Validation of GeoJSON return object schema failed for GET geojson: {str(e)}"
        )
        raise HTTPException(
            status_code=500,
            detail="Return GeoJSON format failed validation. Please contact us!",
        )

    # Always upload to S3 and return presigned URL
    try:
        presigned_url = utils.upload_to_s3_and_get_presigned_url(
            bucket_name=S3_BUCKET,
            prefix=S3_PREFIX_USER_DOWNLOADS,
            data=result,
            input_params=input_params,
            format=format.lower()
        )
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise HTTPException(
            status_code=500, detail="Error uploading to S3. Please contact us!"
        )
    
    return {"download_url": presigned_url}


@router.get("/climate-metadata/{climate_variable}/{ssp}/")
def get_climate_metadata(climate_variable: str, ssp: str) -> Dict:
    """Returns climate metadata JSON blob for given climate_variable and ssp

    Args:
        climate_variable (str): climate variable name
        ssp (str): SSP number

    Returns:
        Dict: JSON blob of climate metadata
    """

    query = sql.SQL(
        "SELECT metadata FROM {schema}.{scenariomip_variable} WHERE variable = %s AND ssp = %s"
    ).format(
        schema=sql.Identifier(config.CLIMATE_SCHEMA_NAME),
        scenariomip_variable=sql.Identifier(config.SCENARIOMIP_VARIABLE_TABLE),
    )

    result = database.execute_query(query=query, params=(climate_variable, ssp))
    result = result[0][0]

    return {"climate_variable": climate_variable,
            "ssp": ssp,
            "metadata": result}
