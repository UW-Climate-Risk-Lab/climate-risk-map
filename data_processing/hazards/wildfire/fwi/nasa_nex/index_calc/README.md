# NASA NEX-GDDP-CMIP6 x Fire Weather Index Pipeline

## Overview

This project processes climate data using Dask, Xarray, and Xclim to compute Fire Weather Index (FWI) components. The workflow integrates AWS S3 for input/output data storage, uses distributed computing, and produces Zarr outputs for efficient climate data access.

The pipeline reads climate model data, calculates indices like `FFMC`, `DMC`, `DC`, and `FWI`, and writes the results back to S3.

Since each model run is independent, containers could be run in parrallel to process many models and scenarios at once.

---

## Project Structure

```
.
├── Dockerfile               # Docker container setup
├── requirements.txt         # Python dependencies
├── constants.py             # Project constants
├── src/                     # Application code
│   ├── __init__.py          # Marks src as a package
│   ├── calc.py              # Calculation logic for FWI
│   ├── pipeline.py          # Main pipeline script
│   └── constants.py         # Constants for input/output configuration
└── README.md                # Project documentation
```

---

## Setup

### Prerequisites

- Docker
- AWS Credentials configured with access to S3
- Python 3.12+ (optional for local development)

---

## Usage

### Building the Docker Image

Build the Docker image:

```bash
docker build --platform linux/amd64 -t wildfire/fwi/nasa_nex .
```

---

### Running the Pipeline

Run the pipeline container with the required arguments:
(This example uses bounds for North America)
```bash
docker run --rm wildfire/fwi/nasa_nex 
    --model MIROC6 
    --scenario ssp126 
    --ensemble_member r1i1p1f1 
    --lat_chunk 30 
    --lon_chunk 72 
    --threads 8 
    --memory_available 16GB
    --x_min 230 --y_min 10 --x_max 300 --y_max 50
```

**Arguments:**

| Argument            | Type   | Description                                  | Required |
|---------------------|--------|----------------------------------------------|----------|
| `--model`           | string | Climate model name (e.g., `CESM2`, `MIROC6`) | Yes      |
| `--scenario`        | string | SSP scenario or `historical`                | Yes      |
| `--ensemble_member` | string | Simulation member (e.g., `r1i1p1f1`)         | Yes      |
| `--lat_chunk`       | int    | Latitude chunk size                         | No       |
| `--lon_chunk`       | int    | Longitude chunk size                        | No       |
| `--threads`         | int    | Threads per worker                          | No       |
| `--memory_available`| string | Memory available per worker (e.g., `16GB`)  | No       |
| `--x_min`, `--y_min`, `--x_max`, `--y_max` | float | Bounding box coordinates for data subset | No |

---

## Pipeline Flow

1. **Input Data Retrieval**: Climate data is fetched from the public S3 bucket `nex-gddp-cmip6`.
2. **Bounding Box Selection**: Optional bounding box filtering of climate data.
3. **Initial Conditions**: Extracts previous year's FFMC, DMC, and DC values if available.
4. **Calculation**: FWI components are computed using the Xclim library.
5. **Zarr Output**: Results are written back to an S3 bucket in Zarr format.
6. **Results Logging**: Execution details are logged to a CSV file in S3.
7. **Cleanup**: Temporary files and data are cleaned up to free resources.

---

## Environment Configuration

### S3 Buckets

- **Input Bucket**: `nex-gddp-cmip6`
- **Output Bucket**: `uw-crl`
- **Output Prefix**: `climate-risk-map/backend/climate/scenariomip`

---

## Code Walkthrough

### `pipeline.py`

- **`main()`**: Orchestrates the pipeline execution.
- **`generate_current_year_config()`**: Generates a configuration for data processing.
- **`s3_uri_exists()`**: Checks for file existence on S3.
- **`InitialConditions.from_zarr()`**: Reads previous year's conditions from a Zarr file.

### `calc.py`

- **`load()`**: Loads input climate data from S3 into memory using Xarray and FSSpec.
- **`calc()`**: Computes FWI components using Xclim.
- **`main()`**: Coordinates the load, calculate, and write steps for each year.

### `constants.py`

Defines constants such as S3 bucket names, input variables, and valid years.

---

## Output

The pipeline writes the calculated results as Zarr files to the specified S3 output location:

```
s3://uw-crl/climate-risk-map/backend/climate/scenariomip/{model}/{scenario}/{ensemble_member}/fwi_day_{year}.zarr
```

---

## Example Output

**Zarr Output File:**

```
fwi_day_MIROC6_ssp126_r1i1p1f1_gn_2060.zarr
```

**CSV Log Entry:**

```
output,n_workers,threads_per_worker,lat_chunk,lon_chunk,load_time,calc_time,write_time
s3://.../fwi_day_2060.zarr,32,8,30,72,12.5,34.2,8.1
```

---

## Local Development

### Running Locally

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Run the script:

   ```bash
   python src/pipeline.py --model MIROC6 --scenario ssp126 --ensemble_member r1i1p1f1
   ```

---

## AWS Batch Processing

The project includes an AWS Batch script (`aws_batch.py`) for parallel processing of multiple climate models and scenarios.

### AWS Batch Script Overview

The AWS Batch script automates the submission and monitoring of multiple pipeline jobs across different models and scenarios. It handles:

- Job submission for all model/scenario combinations
- Monitoring of job status
- Error handling and retries
- Resource management

### Required Environment Variables

| Variable | Description |
|----------|-------------|
| `AWS_REGION` | AWS region for batch jobs |
| `JOB_QUEUE` | AWS Batch job queue name |
| `JOB_DEFINITION` | AWS Batch job definition name |
| `LAT_CHUNK` | Latitude chunk size |
| `LON_CHUNK` | Longitude chunk size |
| `THREADS` | Threads per worker |
| `MEMORY_AVAILABLE` | Memory limit per worker |
| `X_MIN`, `Y_MIN`, `X_MAX`, `Y_MAX` | Bounding box coordinates |
| `TEST` | Boolean flag for test mode |

### Running Batch Jobs

1. Set environment variables:
   ```bash
   export AWS_REGION=us-west-2
   export JOB_QUEUE=your-queue
   export JOB_DEFINITION=your-job-def
   # ...set other required variables...
   ```

2. Run the batch script:
   ```bash
   python aws_batch.py
   ```

### Test Mode

Set `TEST=True` to run a limited subset of jobs for testing:
```bash
export TEST=True
python aws_batch.py
```

---

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for discussion.


## Troubleshooting

1. **ModuleNotFoundError**: Ensure `PYTHONPATH` is set to include the project root.
2. **S3 Access Errors**: Verify AWS credentials are configured correctly.
3. **Missing Data**: Ensure the input files exist in the S3 bucket.