# Climate Data GeoTIFF Processor

A command-line utility for transforming large climate datasets stored in Zarr format into Cloud Optimised GeoTIFFs (COGs) and streaming them directly to Amazon S3.  It supports regional clipping, optional spatial resampling and parallel upload.

## How it works

1. Download/open the input Zarr hierarchy directly from S3 using `xarray`.
2. Normalise longitudes from 0-360 to −180-180 for consistency with GeoJSON boundaries.
3. Optionally clip the data to the GeoJSON polygon for the requested `--region` (files live in `regions/<region>.geojson`).  Pass `--region global` to skip clipping.
4. Handle two temporal data models:
   • **Legacy**: a single `decade_month` dimension (e.g. `"2050-06"`).  
   • **Multi-period hazards**: `year_period` (e.g. `"2015-2044"`) and `return_period` (e.g. `2`, `5`, `100`) with an optional `month_of_year` dimension.
5. (Optional) Resample each clipped tile to a user-specified resolution via nearest, linear or cubic interpolation.
6. Write each slice to a temporary COG using `rioxarray` and immediately upload it to `s3://<bucket>/<prefix>/<region>/`.
7. All slices are processed in parallel with a thread pool whose size is controlled by `--max-workers`.

## Quick-start

```bash
python src/main.py \
  --s3-bucket my-bucket \
  --s3-uri-input s3://my-bucket/climate/fwi_decade_month_ssp126.zarr \
  --s3-prefix-geotiff climate-risk-map/frontend/NEX-GDDP-CMIP6/fwi/ssp126/cogs \
  --region california \
  --variable ensemble_q3         # optional, default shown \
  --output-resolution 0.1        # optional: resample to 0.1° × 0.1° \
  --resampling-method cubic      # nearest | linear | cubic (default: linear) \
  --max-workers 32
```

### Required arguments

| Flag | Description |
|------|-------------|
| `--s3-bucket` | Destination S3 bucket for the generated COGs |
| `--s3-uri-input` | Full S3 URI of the source Zarr dataset |
| `--s3-prefix-geotiff` | Prefix (folder) inside the bucket where files will be placed |
| `--region` | Geographic subset to generate (`global` or a name matching a GeoJSON in `regions/`) |

### Optional arguments

| Flag | Default | Purpose |
|------|---------|---------|
| `--crs` | `4326` | EPSG code of the input dataset |
| `--variable` | `ensemble_q3` | Variable inside the dataset to extract |
| `--x-dim` | `lon` | Name of x/longitude dimension |
| `--y-dim` | `lat` | Name of y/latitude dimension |
| `--geotiff-driver` | `COG` | GDAL driver to use for writing |
| `--max-workers` | `16` | Parallel worker threads |
| `--output-resolution` | _none_ | Target resolution in degrees (e.g. `0.1`) |
| `--resampling-method` | `linear` | `nearest`, `linear`, or `cubic` |

## Output file naming

The processor encodes the temporal slice, region and (if applicable) resolution in the file name:

• **Legacy datasets** (`decade_month`):
```
{variable}-{decade_month}-{region|global}[-{res_deg}deg].tif
```
Example: `fwi_q3-2030-08-california-0p1deg.tif`

• **Multi-period hazard datasets** (`year_period`, `return_period`, optional `month_of_year`):
```
{variable}-{year_period}-{[month_of_year-]}{return_period}-{region|global}[-{res_deg}deg].tif
```
Example annual: `fire_fraction-2015-2044-100-global.tif`
Example monthly: `fire_fraction-2015-2044-7-100-california-0p1deg.tif`

`{res_deg}` is the numeric value of `--output-resolution` with the decimal replaced by `p` (e.g. `0.25` → `0p25`).  It is omitted when no resampling is performed.

## Building & running with Docker

```bash
# Build (for Apple Silicon hosts we target amd64 for GDAL compatibility)
docker build --platform linux/amd64 -t climate/geotiff-processor .

# Run (mount AWS credentials for boto3)
docker run \
  -v ~/.aws/credentials:/root/.aws/credentials:ro \
  climate/geotiff-processor \
  --s3-bucket my-bucket \
  --s3-uri-input s3://my-bucket/climate/fwi_decade_month_ssp126.zarr \
  --s3-prefix-geotiff climate-risk-map/frontend/NEX-GDDP-CMIP6/fwi/ssp126/cogs \
  --region washington
```

## Dependencies

• Python ≥ 3.11  
• GDAL and GEOS (provided via the Docker image)  
• `rioxarray`, `xarray`, `geopandas`, `numpy`, `boto3`, `concurrent-futures`

The project is managed with **Poetry**; run `poetry install` to create a local environment.
