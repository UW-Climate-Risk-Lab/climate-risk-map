from io import BytesIO
import pandas as pd
import os
from pathlib import Path
import hashlib
import base64
import logging
import json

logger = logging.getLogger(__name__)

def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Converts a pandas DataFrame to CSV bytes.

    Args:
        df (pd.DataFrame): The pandas DataFrame to convert

    Returns:
        bytes: The CSV content as bytes, or None if an error occurs
    """
    try:
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue()
        return csv_bytes
    except Exception as e:
        print(f"An error occurred converting DataFrame to CSV bytes: {e}")
        return None

def get_style_fingerprint(asset):
    """Generate a fingerprint hash of styling-related properties of an asset.
    
    Args:
        asset: The asset object
        
    Returns:
        str: A hash representing the current style configuration
    """
    try:
        # Collect properties that affect visual representation
        style_properties = {
            "style": asset.style,
            "custom_color": asset.custom_color,
            "custom_icon": asset.custom_icon,
            "cluster": asset.cluster,
            "data_transformations": asset.data_transformations
        }
        
        # Convert to a stable JSON string and hash it
        style_json = json.dumps(style_properties, sort_keys=True, default=str)
        return hashlib.md5(style_json.encode()).hexdigest()[:8]  # Only use first 8 chars for brevity
    except Exception as e:
        logger.warning(f"Error generating style fingerprint: {e}. Using fallback.")
        # Fallback to a timestamp-based version if JSON serialization fails
        return "v1"

def get_asset_cache_filename(region, asset):
    """Generate a unique filename for caching an asset's geobuf data for a specific region.
    
    Args:
        region: The region object
        asset: The asset object
        
    Returns:
        str: Filename for the cache file
    """
    # Get the style fingerprint to account for style changes
    style_version = get_style_fingerprint(asset)
    
    # Create a unique identifier based on region, asset, and style version
    cache_key = f"{region.name}_{asset.name}_{style_version}"
    
    # Hash the key to create a filename-friendly string
    hashed_key = hashlib.md5(cache_key.encode()).hexdigest()
    return f"{hashed_key}.pbf"

def get_asset_cache_path(region, asset, assets_path):
    """Get the full path for a cached geobuf file.
    
    Args:
        region: The region object
        asset: The asset object
        assets_path: The base path for asset caches
        
    Returns:
        Path: Path object for the cache file
    """
    cache_dir = Path(assets_path) / "geobuf_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / get_asset_cache_filename(region, asset)

def get_asset_cache_url(region, asset, assets_path):
    """Get the URL to the cached geobuf file.
    
    Args:
        region: The region object
        asset: The asset object
        assets_path: The base path for asset caches
        
    Returns:
        str: URL path to the cached file
    """
    filename = get_asset_cache_filename(region, asset)
    return f"/assets/geobuf_cache/{filename}"

def cache_exists(region, asset, assets_path):
    """Check if a cached geobuf file exists for the given region and asset.
    
    Args:
        region: The region object
        asset: The asset object
        assets_path: The base path for asset caches
        
    Returns:
        bool: True if cache exists, False otherwise
    """
    cache_path = get_asset_cache_path(region, asset, assets_path)
    return cache_path.exists()

def write_geobuf_to_cache(region, asset, geobuf_data, assets_path):
    """Write encoded geobuf data to cache file.
    
    Args:
        region: The region object
        asset: The asset object
        geobuf_data: The base64 encoded geobuf data
        assets_path: The base path for asset caches
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        cache_path = get_asset_cache_path(region, asset, assets_path)
        # Decode the base64 data
        binary_data = base64.b64decode(geobuf_data)
        
        # Write the binary data to file
        with open(cache_path, 'wb') as f:
            f.write(binary_data)
        logger.debug(f"Cached geobuf data for {asset.name} in {region.name}")
        return True
    except Exception as e:
        logger.error(f"Error caching geobuf data: {e}")
        return False