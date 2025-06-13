import geopandas as gpd
import psycopg2 # For direct PostgreSQL interaction
from psycopg2 import sql # For safe SQL query construction
import os
import zipfile
import shutil
import logging
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote, urlparse, parse_qs
from io import StringIO, BytesIO # For in-memory CSV buffer
import argparse
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import sys
import pandas as pd # For NaN handling
import tempfile
import time
import datetime

# --- Configuration ---
# This will be the table name in the state-specific or general DB
# Ensure this matches the table name in your SQL CREATE TABLE statement
TABLE_NAME = "fema_nfhl_flood_zones_county"
TARGET_CRS = "EPSG:4326" # Used for GeoPandas reprojection and SRID reference

# Columns to select from shapefile and their target DB names.
# Based on user-provided shapefile columns: Index(, dtype='object')
# EFF_DATE is NOT in this list, so it's removed here.
COLUMNS_TO_KEEP_AND_RENAME = {
    # Shapefile_Column_Name: DB_Column_Name
    "DFIRM_ID": "dfirm_id",
    "FLD_AR_ID": "fld_ar_id",
    "VERSION_ID": "version_id",
    "FLD_ZONE": "flood_zone",
    "ZONE_SUBTY": "flood_zone_subtype",
    "SFHA_TF": "is_sfha",
    "STATIC_BFE": "static_bfe",
    "DEPTH": "flood_depth",
    "geometry": "geom"
}

# Order of columns for insertion into the database using COPY
# This MUST match the order of columns in your target table (excluding any auto-generated ID)
# and the order of data prepared in the CSV buffer.
# Based on your SQL, excluding 'id' (SERIAL) and 'effective_date' (not in shapefile data)
DB_COPY_COLUMNS = [
    "dfirm_id", "fld_ar_id", "version_id", "flood_zone", "flood_zone_subtype",
    "is_sfha", "static_bfe", "flood_depth", "lomr_effective_date", "source_url", "geom"
]


FEMA_BASE_URL = "https://hazards.fema.gov/femaportal/NFHL/"
NFHL_SEARCH_PAGE_URL = urljoin(FEMA_BASE_URL, "searchResult")

STATE_ABBREV_TO_FULLNAME = {
    "AL": "ALABAMA", "AK": "ALASKA", "AZ": "ARIZONA", "AR": "ARKANSAS", "CA": "CALIFORNIA",
    "CO": "COLORADO", "CT": "CONNECTICUT", "DE": "DELAWARE", "FL": "FLORIDA", "GA": "GEORGIA",
    "HI": "HAWAII", "ID": "IDAHO", "IL": "ILLINOIS", "IN": "INDIANA", "IA": "IOWA",
    "KS": "KANSAS", "KY": "KENTUCKY", "LA": "LOUISIANA", "ME": "MAINE", "MD": "MARYLAND",
    "MA": "MASSACHUSETTS", "MI": "MICHIGAN", "MN": "MINNESOTA", "MS": "MISSISSIPPI",
    "MO": "MISSOURI", "MT": "MONTANA", "NE": "NEBRASKA", "NV": "NEVADA", "NH": "NEW HAMPSHIRE",
    "NJ": "NEW JERSEY", "NM": "NEW MEXICO", "NY": "NEW YORK", "NC": "NORTH CAROLINA",
    "ND": "NORTH DAKOTA", "OH": "OHIO", "OK": "OKLAHOMA", "OR": "OREGON", "PA": "PENNSYLVANIA",
    "RI": "RHODE ISLAND", "SC": "SOUTH CAROLINA", "SD": "SOUTH DAKOTA", "TN": "TENNESSEE",
    "TX": "TEXAS", "UT": "UTAH", "VT": "VERMONT", "VA": "VIRGINIA", "WA": "WASHINGTON",
    "WV": "WEST VIRGINIA", "WI": "WISCONSIN", "WY": "WYOMING",
    "AS": "AMERICAN SAMOA", "GU": "GUAM", "MP": "NORTHERN MARIANA ISLANDS",
    "PR": "PUERTO RICO", "VI": "VIRGIN ISLANDS", "DC": "DISTRICT OF COLUMBIA"
}

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

def format_state_name_for_db(state_full_name):
    """Converts a full state name to a lowercase, underscore-separated string."""
    if not state_full_name:
        return None
    return state_full_name.lower().replace(" ", "_").replace("-", "_")

def get_db_config(env_vars, state_filter_abbr=None):
    """
    Reads database configuration from environment variables.
    Overrides dbname if state_filter_abbr is provided.
    """
    db_name_override = None
    if state_filter_abbr:
        state_full_name = STATE_ABBREV_TO_FULLNAME.get(state_filter_abbr.upper())
        if state_full_name:
            db_name_override = format_state_name_for_db(state_full_name)
            logging.info(f"Using state-specific database name: {db_name_override}")
        else:
            logging.warning(f"Unknown state abbreviation '{state_filter_abbr}' for DB name. Will use PG_DBNAME if set.")

    config = {
        "user": env_vars.get("PGUSER"),
        "password": env_vars.get("PGPASSWORD"),
        "host": env_vars.get("PGHOST"),
        "port": env_vars.get("PGPORT", "5432"),
        "dbname": db_name_override if db_name_override else env_vars.get("PG_DBNAME"),
        "schema": env_vars.get("PG_SCHEMA", "climate"),
    }

    required_keys_in_config = ["user", "password", "host", "port", "dbname", "schema"]
    missing_vars = [key for key in required_keys_in_config if not config[key]]
        
    if missing_vars:
        err_msg = (f"Missing required database configuration. Ensure these environment variables are set "
                   f"or a valid --state-filter is provided for DB name: {', '.join(missing_vars)}")
        logging.error(err_msg)
        raise EnvironmentError(err_msg)
    return config

def extract_lomr_effective_date(file_name: str):
    """Extracts an 8-digit effective date from filenames (e.g. '53061C_20220317.zip') and returns it
    as an ISO-formatted date string 'YYYY-MM-DD'. If no valid date is found, returns None."""
    match = re.search(r"_(\d{8})", file_name)
    if match:
        date_str = match.group(1)
        try:
            return datetime.datetime.strptime(date_str, "%Y%m%d").date().isoformat()
        except ValueError:
            return None
    return None

def create_db_connection(db_config):
    """Creates a psycopg2 database connection."""
    try:
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        logging.info(f"Successfully connected to database {db_config['dbname']}.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to database {db_config['dbname']}: {e}")
        raise

def fetch_nfhl_county_download_urls(state_filter_abbr=None):
    """Fetches and optionally filters FEMA NFHL county data download URLs."""
    logging.info(f"Fetching NFHL search page: {NFHL_SEARCH_PAGE_URL}")
    try:
        response = requests.get(NFHL_SEARCH_PAGE_URL, timeout=120)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Error fetching NFHL search page: {e}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')
    raw_urls = []

    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if 'Download/ProductsDownLoadServlet' in href:
            full_url = urljoin(FEMA_BASE_URL, href.replace(" ", "%20"))
            raw_urls.append(full_url)
    
    if not raw_urls:
        logging.warning("No download URLs found on the NFHL search page.")
        return

    unique_urls = sorted(list(set(raw_urls)))
    logging.info(f"Found {len(unique_urls)} unique potential download URLs initially.")

    if state_filter_abbr:
        target_state_full_name = STATE_ABBREV_TO_FULLNAME.get(state_filter_abbr.upper())
        if not target_state_full_name:
            logging.warning(f"Unknown state abbreviation: {state_filter_abbr}. No state filtering will be applied based on URL query params.")
        else:
            logging.info(f"Filtering URLs for state: {target_state_full_name} (from abbreviation: {state_filter_abbr})")
            filtered_urls = []
            for url_str in unique_urls:
                try:
                    parsed_url = urlparse(url_str)
                    query_params = parse_qs(parsed_url.query)
                    state_values_in_url = query_params.get('state') 
                    if state_values_in_url and state_values_in_url[0]:
                        url_state_name = state_values_in_url[0].strip().upper()
                        if url_state_name == target_state_full_name:
                            filtered_urls.append(url_str)
                except Exception as e:
                    logging.debug(f"Could not parse state from URL {url_str} for filtering: {e}")
            
            if not filtered_urls:
                logging.warning(f"No URLs found matching state '{target_state_full_name}' in query parameters.")
                return
            unique_urls = filtered_urls
            logging.info(f"Found {len(unique_urls)} URLs after filtering for state {target_state_full_name}.")

    def sort_key_func(url_str):
        try:
            dfirm_id_match = re.search(r'DFIRMID=([^&]+)', url_str)
            if dfirm_id_match:
                dfirm_id_val = dfirm_id_match.group(1)
                parts = dfirm_id_val.split('_')
                if len(parts) > 1:
                    return int(parts[1]) 
            return 0 
        except (IndexError, ValueError):
            return 0

    sorted_urls = sorted(unique_urls, key=sort_key_func, reverse=True)
    logging.info(f"Sorted URLs. Example of first few: {sorted_urls[:3] if sorted_urls else 'None'}")
    return sorted_urls

def find_flood_hazard_shp_in_memory(zip_content_bytes):
    """Finds the S_Fld_Haz_Ar shapefile within a ZIP file's content (in memory)."""
    try:
        with zipfile.ZipFile(BytesIO(zip_content_bytes), 'r') as zf:
            patterns = [re.compile(r'\.shp$')]
            candidate_files = []
            for item_name in zf.namelist():
                for pattern in patterns:
                    if pattern.search(item_name):
                        candidate_files.append(item_name)
                        if "S_FLD_HAZ_AR.SHP" in item_name.upper(): # Prioritize exact name
                            logging.debug(f"Found specific 'S_Fld_Haz_Ar.shp': {item_name}")
                            return item_name
            
            if candidate_files:
                selected_file = candidate_files[0] # Take the first match if exact not found
                logging.debug(f"Found candidate flood hazard shapefile: {selected_file}")
                return selected_file
            logging.warning("Could not find a flood hazard area shapefile (e.g., S_Fld_Haz_Ar.shp) in the zip content.")
            return None
    except zipfile.BadZipFile:
        logging.error("Bad ZIP content provided.")
        return None
    except Exception as e:
        logging.error(f"Error reading ZIP content: {e}")
        return None

def check_s3_file_exists(s3_client, bucket_name, s3_key):
    """Checks if a file exists in S3."""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        logging.info(f"File s3://{bucket_name}/{s3_key} already exists. Skipping download.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404': # Not found
            return False
        else: # Other error (credentials, permissions, etc.)
            logging.error(f"Error checking S3 for file {s3_key}: {e}")
            raise # Re-raise to halt if it's a critical S3 issue
    except (NoCredentialsError, PartialCredentialsError):
        logging.error("AWS credentials not found for S3 check. Ensure they are configured.")
        raise
    return False


def upload_to_s3(s3_client, bucket_name, s3_key, data_bytes, filename):
    """Uploads data bytes to S3."""
    try:
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=data_bytes)
        logging.info(f"Successfully uploaded {filename} to s3://{bucket_name}/{s3_key}")
        return True
    except (NoCredentialsError, PartialCredentialsError):
        logging.error("AWS credentials not found. Ensure they are configured for Boto3.")
        return False
    except ClientError as e:
        logging.error(f"AWS S3 ClientError uploading {filename}: {e.code} - {e.response.get('Error', {}).get('Message', 'No message')}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during S3 upload of {filename}: {e}")
        return False

def get_zip_content_bytes_from_s3(s3_client, bucket_name, s3_key):
    """
    Retrieves the content of a zip file from S3 as bytes.

    Args:
        s3_client: boto3 client
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the zip file in the S3 bucket.

    Returns:
        bytes: The content of the zip file as bytes, or None if an error occurs.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        zip_content_bytes = response['Body'].read()
        return zip_content_bytes
    except Exception as e:
        print(f"Error retrieving zip file: {e}")
        return None

def load_gdf_to_postgres_copy(gdf, conn, db_schema, target_table_name, db_copy_columns_list, target_crs_epsg):
    """Loads a GeoDataFrame to PostgreSQL using psycopg2's COPY method."""
    if gdf.empty:
        logging.info("GeoDataFrame is empty. Nothing to load.")
        return

    sio = StringIO()
    
    # Prepare DataFrame for CSV export
    # 1. Ensure correct columns and order
    df_for_copy = pd.DataFrame()
    for db_col in db_copy_columns_list:
        if db_col == 'geom':
            # Convert geometry to WKT with SRID
            # ST_GeomFromEWKT is more flexible if SRID is embedded
            df_for_copy[db_col] = gdf['geom'].apply(
                lambda geom: f"SRID={target_crs_epsg};{geom.wkt}" if geom and not geom.is_empty else None
            )
        elif db_col in gdf.columns:
            df_for_copy[db_col] = gdf[db_col]
        else: # Should not happen if COLUMNS_TO_KEEP_AND_RENAME is correct
            logging.warning(f"Expected column '{db_col}' not in GeoDataFrame for COPY. Filling with NULL.")
            df_for_copy[db_col] = pd.NA 

    # 2. Write to CSV buffer
    # Use a unique delimiter like a pipe if data might contain commas/tabs
    # Ensure na_rep is a string that PostgreSQL's COPY understands as NULL (e.g., empty string for text, or specific \N)
    # For CSV, empty unquoted strings are typically NULL for text. For numerics, it's safer to use \N.
    # psycopg2's copy_expert with CSV handles this well.
    df_for_copy.to_csv(sio, sep=',', header=False, index=False, na_rep='', quotechar='"', escapechar='\\')
    sio.seek(0)

    random_id = str(int(time.time() * 1000)) 
    temp_table_name = f"temp_load_{target_table_name}_{random_id}"
    qualified_temp_table_name = sql.SQL("{}").format(sql.Identifier(temp_table_name))
    qualified_table_name = sql.SQL("{}.{}").format(sql.Identifier(db_schema), sql.Identifier(target_table_name))
    
    # For geometry, we are providing EWKT (SRID;WKT). The table column should be `geometry` type.
    # PostGIS will parse the SRID from the EWKT string.
    create_temp_table_sql = sql.SQL("""
                    CREATE TEMP TABLE {temp_table} (
                        dfirm_id TEXT, fld_ar_id TEXT, version_id TEXT, flood_zone TEXT, flood_zone_subtype TEXT,
                        is_sfha TEXT, static_bfe FLOAT, flood_depth FLOAT, lomr_effective_date DATE, source_url TEXT, geom GEOMETRY(MultiPolygon, 4326) NOT NULL
                    );""").format(temp_table=qualified_temp_table_name)

    copy_temp_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '', QUOTE '\"', ESCAPE '\\')").format(
        qualified_temp_table_name,
        sql.SQL(', ').join(map(sql.Identifier, db_copy_columns_list))
    )
    insert_sql = sql.SQL(
                "INSERT INTO {target_table} ({columns}) SELECT {columns} FROM {temp_table} ON CONFLICT DO NOTHING;"
            ).format(
                target_table=qualified_table_name,
                columns=sql.SQL(', ').join(map(sql.Identifier, db_copy_columns_list)),
                temp_table=qualified_temp_table_name
            )
    try:
        with conn.cursor() as cur:
            cur.execute(create_temp_table_sql)
            cur.copy_expert(copy_temp_sql, sio)
            logging.info("Data copied into temp table")
            cur.execute(insert_sql)
            logging.info("Data inserted into final table")
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {temp_table};").format(temp_table=qualified_temp_table_name))
        conn.commit()
        logging.info(f"Successfully loaded {len(gdf)} rows into {db_schema}.{target_table_name} using COPY.")
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Error loading data into {db_schema}.{target_table_name} using COPY: {e}")
        raise
    except Exception as e:
        conn.rollback()
        logging.error(f"Unexpected error during COPY: {e}")
        raise
    finally:
        sio.close()


def process_zip_content(zip_content_bytes, data_url, target_crs_epsg):
    """Extracts, transforms GDF from zip content."""
    shp_in_zip_path = find_flood_hazard_shp_in_memory(zip_content_bytes)
    if not shp_in_zip_path:
        logging.warning(f"No flood hazard shapefile found in zip from {data_url}.")
        return None
    
    temp_dir = None
    try:
        # Create a unique temp directory name to avoid conflicts if run in parallel (though this script is serial)
        temp_dir = tempfile.mkdtemp(prefix="nfhl_shp_")
        
        extracted_shp_path = None
        with zipfile.ZipFile(BytesIO(zip_content_bytes), 'r') as zf:
            # Extract all components of the identified shapefile
            shp_basename_no_ext = os.path.splitext(os.path.basename(shp_in_zip_path))[0]
            shp_dir_in_zip = os.path.dirname(shp_in_zip_path)

            for member_info in zf.infolist():
                member_filename = os.path.basename(member_info.filename)
                member_dir = os.path.dirname(member_info.filename)
                member_basename_no_ext = os.path.splitext(member_filename)[0]

                if member_dir == shp_dir_in_zip and member_basename_no_ext == shp_basename_no_ext:
                    zf.extract(member_info, temp_dir)
            
            extracted_shp_path = os.path.join(temp_dir, shp_in_zip_path)

        if not extracted_shp_path or not os.path.exists(extracted_shp_path):
            logging.error(f"Failed to extract {shp_in_zip_path} from zip of {data_url}")
            return None

        gdf = gpd.read_file(extracted_shp_path)
        logging.info(f"Read {shp_in_zip_path} from {data_url}. Features: {len(gdf)}.")
        
        if gdf.crs:
            gdf = gdf.to_crs(f"EPSG:{target_crs_epsg}")
        else:
            logging.warning(f"GDF from {shp_in_zip_path} has no CRS. Assuming EPSG:{target_crs_epsg}.")
            gdf = gdf.set_crs(f"EPSG:{target_crs_epsg}", allow_override=True)

        # Select and rename columns
        processed_data = {}
        for shp_col, db_col in COLUMNS_TO_KEEP_AND_RENAME.items():
            if shp_col in gdf.columns:
                processed_data[db_col] = gdf[shp_col]
            else:
                logging.debug(f"Column '{shp_col}' (for db col '{db_col}') not in shapefile from {data_url}. Will be NULL.")
                # Create a series of NAs for missing columns to ensure DataFrame structure
                processed_data[db_col] = pd.Series([pd.NA] * len(gdf), index=gdf.index)
        
        # Add source_url
        processed_data['source_url'] = data_url
        
        # Create the final GeoDataFrame with consistent columns
        # Ensure geometry is correctly named and present
        final_gdf = gpd.GeoDataFrame(processed_data, geometry=gdf.geometry, crs=gdf.crs)
        
        # Ensure all DB_COPY_COLUMNS (except geometry, handled by gpd.GeoDataFrame) are present
        for col_name in DB_COPY_COLUMNS:
            if col_name not in final_gdf.columns and col_name!= 'geometry':
                final_gdf[col_name] = pd.NA # Add as NA if missing after processing

        return final_gdf

    except Exception as e:
        logging.error(f"Error processing zip content from {data_url}: {e}", exc_info=True)
        return None
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


def main_etl_pipeline(db_conn, db_schema, db_name, county_data_urls, s3_bucket, s3_prefix, target_crs_epsg_str):
    """Main ETL pipeline: download, S3 upload, process, and load to PostGIS."""
    s3_client = boto3.client('s3')
    all_gdfs_for_state = [] # Accumulate GDFs if loading all at once, or load one by one

    for i, data_url in enumerate(county_data_urls):
        logging.info(f"Processing county URL {i+1}/{len(county_data_urls)}: {data_url}")
        
        parsed_url_for_filename = urlparse(data_url)
        query_params_for_filename = parse_qs(parsed_url_for_filename.query)
        file_name_param_list = query_params_for_filename.get('fileName')
        
        if file_name_param_list:
            zip_filename = file_name_param_list[0]
        else: 
            dfirmid_param_list = query_params_for_filename.get('DFIRMID')
            dfirmid_val = dfirmid_param_list[0] if dfirmid_param_list else "unknown_dfirmid"
            zip_filename = f"{dfirmid_val}.zip"
        zip_filename = re.sub(r'[^\w._-]+', '_', zip_filename)  # Sanitize

        s3_key = f"{s3_prefix.rstrip('/')}/{db_name}/{zip_filename}" if s3_prefix else zip_filename
        
        # We want to cache these files in S3 in case they are taken offline in the future.
        # If the cached file already exists, read bytes directly from that
        if check_s3_file_exists(s3_client, s3_bucket, s3_key):
            logging.info(f"File {s3_key} exists in S3. Downloading directly from S3")
            zip_content_bytes = get_zip_content_bytes_from_s3(s3_client, s3_bucket, s3_key)
        else:
            try:
                logging.info(f"Downloading: {zip_filename} from {data_url}...")
                response = requests.get(data_url, timeout=300)
                response.raise_for_status()
                zip_content_bytes = response.content
                logging.info(f"Downloaded {round(len(zip_content_bytes) / 1000000, 1)} MB for {zip_filename}.")
            except requests.RequestException as e:
                logging.error(f"Error downloading {data_url}: {e}")
            uploaded = upload_to_s3(s3_client, s3_bucket, s3_key, zip_content_bytes, zip_filename)
            if not uploaded:
                logging.warning(f"S3 upload failed for {zip_filename}")

        # Process the downloaded content
        gdf = process_zip_content(zip_content_bytes, data_url, target_crs_epsg_str)
        if gdf is not None and not gdf.empty:
            # Extract LOMR effective date from the filename (if present)
            lomr_date_iso = extract_lomr_effective_date(zip_filename)
            # Add the effective date column to every row in this GeoDataFrame
            gdf['lomr_effective_date'] = lomr_date_iso
            all_gdfs_for_state.append(gdf)
        else:
            logging.info(f"No data processed from {data_url} for DB loading.")

    # After processing all URLs, combine GDFs and load
    if all_gdfs_for_state:
        logging.info(f"Combining {len(all_gdfs_for_state)} GeoDataFrames for bulk load...")
        
        # Ensure all GDFs have the same columns in the same order before concat
        # This is critical if some shapefiles were missing optional columns
        # DB_COPY_COLUMNS includes 'geometry' implicitly handled by GeoDataFrame
        # and 'source_url' added during processing.
        
        # Create a master list of columns expected in the GDFs before concat
        # These are the renamed DB columns from COLUMNS_TO_KEEP_AND_RENAME + 'lomr_effective_date' + 'source_url'
        expected_gdf_cols = list(COLUMNS_TO_KEEP_AND_RENAME.values()) + ['lomr_effective_date', 'source_url']

        consistent_gdfs = []
        for gdf_item in all_gdfs_for_state:
            for col in expected_gdf_cols:
                if col not in gdf_item.columns:
                    gdf_item[col] = pd.NA # Add missing columns as NA
            # Reorder columns to match expected_gdf_cols, plus geometry
            # Ensure 'geometry' column is correctly handled by GeoDataFrame itself
            current_cols_ordered = [c for c in expected_gdf_cols if c in gdf_item.columns]
            consistent_gdfs.append(gdf_item[current_cols_ordered + ['geometry']])

        if not consistent_gdfs:
            logging.warning("No GeoDataFrames to load after consistency check for concatenation.")
            return

        combined_gdf = pd.concat(consistent_gdfs, ignore_index=True)
        combined_gdf = combined_gdf.dropna(subset=["fld_ar_id"])
        combined_gdf = combined_gdf.dropna(subset=["geom"]) # If geometry is not present, we can't use it
        
        # Define uniqueness based on the core attributes matching the UNIQUE index.
        unique_subset = [
            'dfirm_id', 'fld_ar_id','version_id', 'flood_zone', 'flood_zone_subtype', 
            'is_sfha', 'static_bfe', 'flood_depth', 'lomr_effective_date'
        ]
        combined_gdf = combined_gdf.drop_duplicates(subset=unique_subset)
        
        if combined_gdf.empty:
            logging.info("Combined GeoDataFrame is empty. No data to load to PostGIS.")
            return
            
        logging.info(f"Total features in combined GeoDataFrame: {len(combined_gdf)}")
        
        # Ensure the 'geometry' column in combined_gdf is active geometry
        if 'geometry' not in combined_gdf.columns or not isinstance(combined_gdf, gpd.GeoDataFrame):
             combined_gdf = gpd.GeoDataFrame(combined_gdf, geometry='geometry', crs=f"EPSG:{target_crs_epsg_str}")
        elif combined_gdf.geometry.name!= 'geometry': # if geometry column has a different name
             combined_gdf = combined_gdf.set_geometry('geometry', crs=f"EPSG:{target_crs_epsg_str}")


        # Perform the bulk load using psycopg2 COPY
        try:
            # Ensure table exists and is empty if doing a full refresh, or append.
            # For this script, we assume the table exists. If it's a full refresh,
            # user should TRUNCATE before running.
            # Example: Clear table before loading all counties for a state (optional)
            # with db_conn.cursor() as cur:
            #     logging.info(f"Truncating table {db_schema}.{TABLE_NAME} before load.")
            #     cur.execute(sql.SQL("TRUNCATE TABLE {}.{} RESTART IDENTITY;").format(
            #         sql.Identifier(db_schema), sql.Identifier(TABLE_NAME)
            #     ))
            # db_conn.commit()

            load_gdf_to_postgres_copy(combined_gdf, db_conn, db_schema, TABLE_NAME, DB_COPY_COLUMNS, target_crs_epsg_str)
        except Exception as e:
            logging.error(f"Failed during bulk load to PostGIS: {e}", exc_info=True)
    else:
        logging.info("No new data processed or downloaded to load into PostGIS.")


def main():
    """Main function to orchestrate the ETL process."""
    parser = argparse.ArgumentParser(description="Download FEMA NFHL county data, upload to S3, and load into PostGIS.")
    parser.add_argument("--s3-bucket", required=True, help="AWS S3 bucket name for storing downloaded ZIP files.")
    parser.add_argument("--s3-prefix", required=True, help="AWS S3 prefix (folder path) within the bucket.")
    parser.add_argument("--state-filter", required=False, default=None, 
                        help="Optional: State abbreviation (e.g., CA, NY) to filter downloads. "
                             "If provided, also determines the database name (e.g., 'california', 'new_york'). "
                             "If not provided, PG_DBNAME env var is used for database name.")
    
    args = parser.parse_args()

    logging.info("Starting NFHL county data processing script.")
    logging.info(f"S3 Bucket: {args.s3_bucket}, S3 Prefix: {args.s3_prefix}")
    if args.state_filter:
        logging.info(f"State Filter Applied: {args.state_filter.upper()}")

    db_conn = None
    try:
        db_config = get_db_config(os.environ, state_filter_abbr=args.state_filter)
        db_conn = create_db_connection(db_config)
        
        target_crs_epsg_str = TARGET_CRS.split(':')[1] # Get '4326' from 'EPSG:4326'

        county_data_urls = fetch_nfhl_county_download_urls(state_filter_abbr=args.state_filter)
        
        if not county_data_urls:
            logging.warning("No county data URLs to process. Exiting.")
            sys.exit(0)

        logging.info(f"Retrieved {len(county_data_urls)} county data URLs to process.")
        
        main_etl_pipeline(db_conn, db_config["schema"], db_config["dbname"], county_data_urls, 
                          args.s3_bucket, args.s3_prefix, target_crs_epsg_str)
        
        logging.info("NFHL county data processing script finished successfully.")

    except EnvironmentError as e:
        logging.critical(f"Configuration error: {e}")
        sys.exit(1)
    except psycopg2.Error as e:
        logging.critical(f"Database operational error: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if db_conn:
            db_conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()