import json
import uuid
from typing import Any, Dict, List
import logging
import gzip
import io
import csv
from collections import defaultdict

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from geojson_pydantic import FeatureCollection
from geojson_pydantic.features import Feature
from geojson_pydantic.geometries import Polygon
from fastapi import HTTPException

from . import schemas

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SSM = boto3.client("ssm")

def create_bbox(bboxes: List[schemas.BoundingBox]) -> FeatureCollection:
    """Creates GeoJSON spec. object from list of Bounding Boxes

    Args:
        bboxes (List[schemas.BoundingBox]): List of BoundingBox objects (see schemas.py)

    Returns:
        FeatureCollection: GeoJSON spec object
    """
    features = []

    for bbox in bboxes:
        # Create a Polygon geometry from bounding box
        polygon = Polygon.from_bounds(**bbox.model_dump())

        # Create a Feature with the Polygon geometry and empty properties
        feature = Feature(geometry=polygon, properties={}, type="Feature")

        features.append(feature)

    # Create and return the FeatureCollection
    return FeatureCollection(features=features, type="FeatureCollection")


def clean_geojson_data(raw_geojson: Dict[str, Any]) -> Dict[str, Any]:
    """Condense feature property fields to avoid duplicate features in return data

    We condense city and county since some features can span multiple (i.e long power transmission lines)
    Args:
        raw_geojson (Dict[str, Any]): Output of the query

    Returns:
        Dict[str, Any]: GeoJSON data with condensed climate and city fields
    """
    features = raw_geojson.get("features", [])
    aggregated_features = {}
    present_osm_ids = set()

    for feature in features:
        properties = feature.get("properties", {})
        osm_id = properties.get("osm_id")

        if osm_id is None:
            continue  # Skip features without an osm_id

        # Initialize the aggregated feature if it doesn't exist
        if osm_id not in present_osm_ids:
            # Deep copy to avoid mutating the original feature
            aggregated_features[osm_id] = {
                "type": feature.get("type"),
                "geometry": feature.get("geometry"),
                "properties": {
                    k: v
                    for k, v in properties.items()
                    if k
                    not in [
                        "geometry_wkt",
                        "latitude",
                        "longitude",
                        "county",
                        "city",
                    ]
                },
            }
            aggregated_features[osm_id]["properties"]["city"] = []
            aggregated_features[osm_id]["properties"]["county"] = []

            present_osm_ids.add(osm_id)

        city = properties.get("city")
        county = properties.get("county")

        if city not in aggregated_features[osm_id]["properties"]["city"]:
            aggregated_features[osm_id]["properties"]["city"].append(city)

        if county not in aggregated_features[osm_id]["properties"]["county"]:
            aggregated_features[osm_id]["properties"]["county"].append(county)

    # Convert the aggregated features back into a FeatureCollection
    new_features = list(aggregated_features.values())

    return {"type": "FeatureCollection", "features": new_features}


def create_descriptive_filename(params: schemas.GetDataInputParameters, format: str) -> str:
    """Creates a descriptive filename based on query parameters

    Args:
        params (schemas.GetDataInputParameters): Input parameters
        format (str): File format

    Returns:
        str: Descriptive filename
    """
    # Start with base components
    parts = [params.osm_category, params.osm_types[0]]
    
    # Add optional components if present
    if params.osm_subtypes:
        parts.append(f"subtype-{'-'.join(params.osm_subtypes)}")
    
    if params.climate_variable:
        parts.append(f"climate-{params.climate_variable}")
        parts.append(f"ssp{params.climate_ssp}")
        if len(params.climate_decade) == 1:
            parts.append(f"{params.climate_decade[0]}s")
        
    # Add timestamp for uniqueness
    parts.append(str(uuid.uuid4())[:8])
    
    # Join parts and add extension
    filename = "_".join(parts)
    if format == "csv":
        return f"{filename}.csv.gz"
    return f"{filename}.{format}"

def upload_to_s3_and_get_presigned_url(
    bucket_name: str, 
    prefix: str, 
    data: dict,
    input_params: schemas.GetDataInputParameters, 
    format: str = "geojson",
    expiration: int = 3600
) -> str:
    """
    Uploads data to S3 and returns a presigned URL.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The name of the S3 prefix.
        data (dict): The data to upload.
        input_params (schemas.GetDataInputParameters): Input parameters for descriptive filename
        format (str): File format (geojson or csv).
        expiration (int): Time in seconds for the presigned URL to remain valid.

    Returns:
        str: The presigned URL.
    """
    s3_client = boto3.client("s3")
    
    # Generate descriptive filename
    filename = create_descriptive_filename(input_params, format)
    object_key = prefix + filename

    try:
        # Prepare content based on format
        if format == "csv":
            content = geojson_to_csv_buffer(data).getvalue()
            content_type = "application/gzip"
        else:
            content = json.dumps(data)
            content_type = "application/json"

        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=content,
            ContentType=content_type
        )
        # Generate presigned URL
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_key},
            ExpiresIn=expiration
        )
        return presigned_url
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise HTTPException(
            status_code=500, detail="Error uploading to S3. Please contact us!"
        )

def get_parameter(name):
    return SSM.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']

def geojson_to_wkt(geojson):
    """
    Convert a GeoJSON-like dictionary to a WKT string.
    
    Args:
        geojson (dict): GeoJSON geometry field dictionary with `type` and `coordinates`.
        
    Returns:
        str: WKT string representation of the geometry.
    """
    geometry_type = geojson['type']
    coordinates = geojson['coordinates']

    def format_coords(coords):
        if isinstance(coords[0], (list, tuple)):  # Handle nested coordinates (e.g., LineString, Polygon)
            return ', '.join(format_coords(c) if isinstance(c[0], (list, tuple)) else ' '.join(map(str, c)) for c in coords)
        return ' '.join(map(str, coords))  # Handle single coordinate pair (e.g., Point)

    if geometry_type == 'Point':
        return f"POINT ({format_coords(coordinates)})"
    elif geometry_type == 'LineString':
        return f"LINESTRING ({format_coords(coordinates)})"
    elif geometry_type == 'Polygon':
        return f"POLYGON (({format_coords(coordinates)}))"
    elif geometry_type == 'MultiPoint':
        return f"MULTIPOINT ({format_coords(coordinates)})"
    elif geometry_type == 'MultiLineString':
        return f"MULTILINESTRING ({', '.join(f'({format_coords(line)})' for line in coordinates)})"
    elif geometry_type == 'MultiPolygon':
        return f"MULTIPOLYGON ({', '.join(f'(({format_coords(polygon)}))' for polygon in coordinates)})"
    elif geometry_type == 'GeometryCollection':
        return f"GEOMETRYCOLLECTION ({', '.join(geojson_to_wkt(geom) for geom in geojson['geometries'])})"
    else:
        return ''

def geojson_to_csv_buffer(geojson_data: dict) -> io.BytesIO:
    """Converts GeoJSON to gzipped CSV efficiently

    Args:
        geojson_data (dict): GeoJSON object

    Returns:
        io.BytesIO: Gzipped CSV buffer
    """
    # Create in-memory buffer for gzipped CSV
    buffer = io.BytesIO()
    
    # Extract features
    features = geojson_data.get('features', [])
    if not features:
        return buffer
    
    # Collect all possible property keys.
    properties_headers = set()
    
    # First pass - collect all property keys
    for feature in features:
        if feature.get('properties'):
            properties_headers.update(feature['properties'].keys())

    properties_headers.update(["geometry_wkt"])

    # Define the desired order of columns
    core_columns = ['osm_id', 'osm_type']
    if 'osm_subtype' in properties_headers:
        core_columns.append('osm_subtype')  # Include `osm_subtype` only if it exists
    geo_columns = ['geometry_wkt']
    time_columns = ['month', 'decade', 'ssp']
    ensemble_columns = [
        'ensemble_mean', 'ensemble_median', 'ensemble_min',
        'ensemble_max', 'ensemble_stddev', 'ensemble_q1', 'ensemble_q3'
    ]
    
    # Combine the predefined order
    desired_order = core_columns + geo_columns + time_columns + ensemble_columns
    
    # Identify any additional columns not in the desired order
    additional_columns = [col for col in properties_headers if col not in desired_order]
    
    # Final column order
    final_order = desired_order + additional_columns
    
    # Open gzip writer
    with gzip.GzipFile(mode='wb', fileobj=buffer) as gz:
        writer = csv.DictWriter(io.TextIOWrapper(gz, encoding='utf-8', newline=''), 
                              fieldnames=final_order)
        writer.writeheader()

        # Second pass - write data
        for feature in features:
            row = defaultdict(str)
            row["geometry_wkt"] = geojson_to_wkt(feature["geometry"])
            props = feature.get('properties', {})
            
            # Add all other properties
            for key, value in props.items():
                if key not in row:
                    row[key] = value if value is not None else ''
                    
            writer.writerow(row)
    
    buffer.seek(0)
    return buffer