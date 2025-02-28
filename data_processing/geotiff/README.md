# Climate Data Geotiff Processor

This component processes climate data from Zarr format into state-specific GeoTIFF files and uploads them to S3.

## Overview

The processor takes climate data stored in Zarr format and:
1. Clips it to state boundaries using GeoJSON masks
2. Converts it to Cloud Optimized GeoTIFF (COG) format
3. Uploads the resulting files to S3

## Important Note on Time Dimension

The current implementation expects the input dataset to have a `decade_month` dimension (e.g., "2030-08", "2030-09") for processing decadal-scale data. Each unique combination of variable and decade-month will generate a separate GeoTIFF file.

## Usage

```bash
python src/main.py \
    --s3-bucket my-bucket \
    --s3-uri-input s3://my-bucket/climate-data/dataset.zarr \
    --s3-prefix-geotiff climate-data/geotiffs \
    --state California
```

### Optional Arguments

- `--crs`: Coordinate Reference System (default: "4326")
- `--x-dim`: X dimension name in dataset (default: "lon")
- `--y-dim`: Y dimension name in dataset (default: "lat")
- `--geotiff-driver`: GeoTIFF driver to use (default: "COG")
- `--max-workers`: Maximum parallel workers (default: 16)

## Output Format

Files are named using the pattern: `{variable}-{decade_month}-{state}.tif`

Example: `temperature-2030-08-california.tif`
