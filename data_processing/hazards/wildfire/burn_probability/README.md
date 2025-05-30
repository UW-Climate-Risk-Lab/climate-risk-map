# Burn Probability Processing

This repository contains tools to aggregate and reproject USDA Forest Service Burn Probability data into Zarr format for the Climate Risk Map backend.

## Data Source
Data is sourced from the USDA Forest Service RDS archive:  
https://www.fs.usda.gov/rds/archive/catalog/RDS-2020-0016-2

## Requirements
- Python 3.13+
- Poetry (recommended) or pip
- Access to an S3 bucket (environment variable `S3_BUCKET` set)

## Installation

### Using Poetry
```bash
uv sync
```

```bash
# after activating uv/.venv
uv run src/main.py
```

This will:
1. Read `./src/data/BP_CONUS/BP_CONUS.tif`  
2. Aggregate to the specified km resolution  
3. Reproject to EPSG:4326 with lon in 0–360°  
4. Write result to S3 at `s3://$S3_BUCKET/climate-risk-map/backend/climate/usda/BP_CONUS.zarr`
