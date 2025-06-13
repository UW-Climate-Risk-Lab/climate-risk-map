# FEMA NFHL County Flood Data ETL and Analysis Pipeline

## Table of Contents
- [Overview](#overview)
- [Key Features](#key-features)
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Database Configuration](#database-configuration)
- [Running the Script](#running-the-script)
- [Understanding the Data](#understanding-the-data)
- [Docker Support](#docker-support)
- [Troubleshooting](#troubleshooting)
- [Data Sources and References](#data-sources-and-references)

## Overview

This document describes a comprehensive Python-based ETL (Extract, Transform, Load) pipeline designed to process county-level flood hazard data from the Federal Emergency Management Agency (FEMA) National Flood Hazard Layer (NFHL). The pipeline automates the discovery, downloading, caching, and processing of flood hazard data into a PostgreSQL/PostGIS database for flood risk analysis.

**Key Capabilities:**
- **Intelligent Data Discovery**: Automatically scrapes FEMA's NFHL search results to identify available county-level flood data
- **S3 Caching & Archival**: Implements smart caching in AWS S3 to avoid redundant downloads and preserve data availability
- **State-Specific Processing**: Filter processing by state with automatic database name derivation
- **LOMR Date Extraction**: Automatically extracts Letter of Map Revision (LOMR) effective dates from filenames
- **Robust Data Loading**: Uses PostgreSQL COPY with temporary tables for efficient bulk loading
- **Deduplication**: Prevents duplicate records based on key flood zone attributes
- **Error Handling & Logging**: Comprehensive logging and graceful error handling

## Key Features

### 🔍 **Smart Data Discovery**
- Scrapes FEMA NFHL search result pages to identify download links
- Supports filtering by state abbreviation (e.g., "CA", "NY", "TX")
- Automatically sorts and prioritizes the most recent data versions

### 💾 **S3 Caching Strategy**
- **Check-First Policy**: Always checks if files exist in S3 before downloading from FEMA
- **Automatic Upload**: Downloads from FEMA are automatically cached to S3 for future use
- **Organized Storage**: Files are stored with structure: `{prefix}/{database_name}/{filename}`
- **Resilience**: Protects against FEMA data availability issues by maintaining local copies

### 📊 **Advanced Data Processing**
- **Automatic CRS Conversion**: Reprojects all data to EPSG:4326 (WGS84) for consistency
- **LOMR Date Extraction**: Parses effective dates from filenames (e.g., `53061C_20220317.zip` → `2022-03-17`)
- **Column Mapping & Cleanup**: Maps shapefile columns to standardized database column names
- **Deduplication**: Removes duplicate records based on key attributes

### 🗄️ **Efficient Database Loading**
- **Bulk Loading**: Uses PostgreSQL COPY for high-performance data insertion
- **Temporary Table Strategy**: Loads data through temporary tables for better error handling
- **State-Specific Databases**: Automatically creates database names from state filters (e.g., `california`, `new_york`)
- **Schema Support**: Configurable database schema (defaults to `climate`)

## Prerequisites

### Software Requirements
- **Python 3.7+**
- **PostgreSQL** with **PostGIS extension** enabled
- **AWS Account** with S3 access and proper IAM permissions

### Python Dependencies
Install via pip or use the provided `pyproject.toml`:

```bash
pip install geopandas psycopg2-binary requests beautifulsoup4 boto3 pandas
```

Or if using the project structure:
```bash
uv sync  # if using uv
# or
pip install -r requirements.txt
```

### AWS Configuration
Ensure AWS credentials are configured via one of:
- `~/.aws/credentials` file
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- IAM role (if running on EC2)
- AWS CLI: `aws configure`

## Environment Setup

### Required Environment Variables

```bash
# Database Connection (Required)
export PGUSER="your_postgresql_username"
export PGPASSWORD="your_postgresql_password"
export PGHOST="your_postgresql_host"
export PGPORT="5432"  # Optional, defaults to 5432

# Database Configuration
export PG_DBNAME="your_default_database_name"  # Used when no --state-filter provided
export PG_SCHEMA="climate"  # Optional, defaults to 'climate'

# AWS Configuration (if not using ~/.aws/credentials)
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_DEFAULT_REGION="us-east-1"  # or your preferred region
```

### Example `.env` file:
```bash
# Database
PGUSER=postgres
PGPASSWORD=your_secure_password
PGHOST=localhost
PGPORT=5432
PG_DBNAME=flood_data
PG_SCHEMA=climate

# AWS
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
```

## Database Configuration

### Target Table Schema

The script loads data into a table named `fema_nfhl_flood_zones_county`. Create this table in your target database:

```sql
-- Create the target table (adjust schema name as needed)
CREATE TABLE IF NOT EXISTS climate.fema_nfhl_flood_zones_county (
    id SERIAL PRIMARY KEY,
    dfirm_id TEXT,
    fld_ar_id TEXT,
    version_id TEXT,
    flood_zone TEXT,
    flood_zone_subtype VARCHAR(75),
    is_sfha VARCHAR(5),
    static_bfe NUMERIC(10, 2),
    flood_depth NUMERIC(8, 2),
    lomr_effective_date DATE,  -- LOMR effective date extracted from filename
    source_url TEXT,
    geom geometry(MultiPolygon, 4326)  -- Geometry in WGS84
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_fema_nfhl_flood_zones_county_dfirm_id_version_id
    ON climate.fema_nfhl_flood_zones_county (dfirm_id, version_id);

CREATE INDEX IF NOT EXISTS idx_fema_nfhl_flood_zones_county_flood_zone
    ON climate.fema_nfhl_flood_zones_county (flood_zone);

CREATE INDEX IF NOT EXISTS idx_fema_nfhl_flood_zones_county_geom
    ON climate.fema_nfhl_flood_zones_county
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_fema_nfhl_flood_zones_county_lomr_date
    ON climate.fema_nfhl_flood_zones_county (lomr_effective_date);
```

### Column Mapping

The script maps shapefile columns to database columns as follows:

| Shapefile Column | Database Column | Description |
|------------------|-----------------|-------------|
| `DFIRM_ID` | `dfirm_id` | Digital Flood Insurance Rate Map identifier |
| `FLD_AR_ID` | `fld_ar_id` | Flood Area identifier |
| `VERSION_ID` | `version_id` | Version of the FIRM data |
| `FLD_ZONE` | `flood_zone` | Primary flood zone designation (A, AE, X, VE, etc.) |
| `ZONE_SUBTY` | `flood_zone_subtype` | Flood zone subtype (FLOODWAY, etc.) |
| `SFHA_TF` | `is_sfha` | Special Flood Hazard Area flag (T/F) |
| `STATIC_BFE` | `static_bfe` | Static Base Flood Elevation |
| `DEPTH` | `flood_depth` | Flood depth for shallow flooding zones |
| `geometry` | `geom` | Polygon geometry (converted to MultiPolygon) |

**Additional Columns Added by Script:**
- `lomr_effective_date`: Extracted from filename patterns like `53061C_20220317.zip`
- `source_url`: FEMA download URL for traceability

## Running the Script

### Command Line Arguments

```bash
python main.py --s3-bucket BUCKET_NAME --s3-prefix PREFIX [--state-filter STATE]
```

**Arguments:**
- `--s3-bucket` (Required): AWS S3 bucket name for storing downloaded ZIP files
- `--s3-prefix` (Required): S3 prefix/folder path within the bucket
- `--state-filter` (Optional): Two-letter state abbreviation for filtering

### Usage Examples

#### Process All Available Counties
```bash
python main.py \
    --s3-bucket "my-fema-flood-data" \
    --s3-prefix "nfhl_raw_zips/county_level"
```
*Uses `PG_DBNAME` environment variable for database name*

#### Process Specific State (California)
```bash
python main.py \
    --s3-bucket "my-fema-flood-data" \
    --s3-prefix "nfhl_raw_zips/county_level" \
    --state-filter CA
```
*Automatically uses database name: `california`*

#### Process New York State
```bash
python main.py \
    --s3-bucket "my-fema-flood-data" \
    --s3-prefix "nfhl_raw_zips/county_level" \
    --state-filter NY
```
*Automatically uses database name: `new_york`*

### S3 File Organization

Files are organized in S3 as:
```
my-bucket/
└── nfhl_raw_zips/county_level/
    ├── california/
    │   ├── 06001C_20220815.zip
    │   ├── 06003C_20220901.zip
    │   └── ...
    ├── new_york/
    │   ├── 36001C_20220715.zip
    │   └── ...
    └── texas/
        └── ...
```

## Understanding the Data

### FEMA Flood Zone Classifications

The script processes FEMA flood zones that represent different levels of flood risk:

#### **Special Flood Hazard Areas (SFHAs)** - High Risk
- **Zone A**: 1% annual chance flood areas without base flood elevations
- **Zone AE**: 1% annual chance flood areas WITH base flood elevations
- **Zone AO**: Areas of shallow flooding (sheet flow) with average depths 1-3 feet
- **Zone AH**: Areas of shallow ponding with flood depths 1-3 feet
- **Zone V**: Coastal areas with wave action and 1% annual chance of flooding
- **Zone VE**: Coastal areas with wave action and base flood elevations

#### **Moderate to Low Risk Areas**
- **Zone X (Shaded)**: Areas between 1% and 0.2% annual chance flood (moderate risk)
- **Zone X (Unshaded)**: Areas of minimal flood hazard (low risk)

#### **Undetermined Risk**
- **Zone D**: Areas of possible but undetermined flood hazards

### Key Data Fields for Analysis

- **`flood_zone`**: Primary classification for risk assessment
- **`is_sfha`**: Quick identifier for high-risk areas requiring flood insurance
- **`static_bfe`**: Critical for determining flood depth at specific locations
- **`flood_zone_subtype`**: Identifies regulatory floodways (most restrictive areas)
- **`lomr_effective_date`**: When flood map changes became effective

## Docker Support

The project includes Docker support for consistent deployment:

### Building the Docker Image
```bash
docker build -t fema-nfhl-etl .
```

### Running with Docker
```bash
docker run --rm \
  -e PGUSER=your_user \
  -e PGPASSWORD=your_password \
  -e PGHOST=your_host \
  -e PG_DBNAME=your_db \
  -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  fema-nfhl-etl \
  --s3-bucket "your-bucket" \
  --s3-prefix "your-prefix" \
  --state-filter CA
```

## Troubleshooting

### Common Issues

#### **Database Connection Errors**
- Verify all `PG*` environment variables are set correctly
- Ensure PostgreSQL server is accessible from your network
- Check that PostGIS extension is installed: `CREATE EXTENSION IF NOT EXISTS postgis;`

#### **AWS S3 Permission Errors**
- Verify AWS credentials are configured
- Ensure S3 bucket exists and you have read/write permissions
- Check bucket policy allows your IAM user/role access

#### **No Data URLs Found**
- FEMA website structure may have changed
- Check if `NFHL_SEARCH_PAGE_URL` is still valid
- Verify state abbreviation is correct (use 2-letter codes like 'CA', 'NY')

#### **Shapefile Processing Errors**
- Some ZIP files may not contain the expected `S_Fld_Haz_Ar.shp`
- Script will log warnings and skip problematic files
- Check logs for specific file processing errors

### Logging

The script provides detailed logging:
- **INFO**: General progress and status updates
- **WARNING**: Non-critical issues (missing files, skipped data)
- **ERROR**: Processing errors for specific files
- **CRITICAL**: Fatal errors that stop execution

## Data Sources and References

### Official FEMA Resources
- **[FEMA Map Service Center](https://msc.fema.gov/)**: Primary portal for NFHL data and FIRMs
- **[NFHL Viewer](https://hazards.fema.gov/femaportal/NFHL/)**: Interactive online flood hazard viewer
- **[FIRM Database Technical Reference](https://www.fema.gov/flood-maps/tools-resources/engineering-library)**: Comprehensive schema documentation
- **[OpenFEMA API](https://www.fema.gov/about/openfema)**: Additional FEMA datasets via API

### Data Update Frequency
- **County-level data**: Updated as new flood studies are completed or LOMRs become effective
- **State-level data**: May be updated bi-weekly
- **NFHL data**: Dynamic and continuously updated as new studies are published

### Integration Opportunities

This processed NFHL data provides a foundation for:
- **Property Risk Assessment**: Join with parcel data using spatial relationships
- **Infrastructure Vulnerability**: Identify critical facilities in flood zones
- **Insurance Analysis**: Determine SFHA requirements for properties
- **Emergency Planning**: Prioritize evacuation routes and shelter locations
- **Climate Risk Modeling**: Baseline data for sea level rise and climate change impacts
- **Economic Impact Analysis**: Calculate potential flood damages by zone

---

*This ETL pipeline provides authoritative FEMA flood hazard data in a format optimized for geospatial analysis and flood risk assessment workflows.*