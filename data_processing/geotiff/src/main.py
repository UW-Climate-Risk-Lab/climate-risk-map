import logging
import argparse
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import rioxarray
import geopandas as gpd
import xarray as xr
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class GeotiffTask:
    """Data class to represent a geotiff processing task"""

    da: xr.DataArray
    output_path: Path
    variable: str
    decade_month: str
    s3_bucket: str
    s3_prefix: str
    geotiff_driver: str


class S3Handler:
    """Class to handle AWS S3 operations"""

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = boto3.client("s3")

    def upload_file(self, local_path: Path, s3_key_prefix: str):
        "Upload a single file to s3"
        try:
            s3_key = str(Path(s3_key_prefix) / local_path.name)
            self.client.upload_file(str(local_path), self.bucket_name, s3_key)
            logger.debug(
                f"Uploaded {local_path.name} to s3://{self.bucket_name}/{s3_key}"
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {local_path}: {str(e)}")
            return False


class StateGeometryFetcher:
    """Class responsible for fetching and processing USA geometry data"""

    STATES_GEOJSON_PATH = "assets/States.json"
    USA_GEOJSON_PATH = "assets/US.geojson"

    @staticmethod
    def get_state_geometry(state: str) -> gpd.GeoDataFrame:

        if state.lower() == "usa":
            gdf = gpd.read_file(StateGeometryFetcher.USA_GEOJSON_PATH)
            return gdf

        # Handle individual state
        gdf = gpd.read_file(StateGeometryFetcher.STATES_GEOJSON_PATH)

        # Normalize state name for matching
        normalized_state = state.lower().replace("-", " ").replace("_", " ")

        # Create lowercase name column for matching
        gdf["name"] = gdf["NAME"].str.lower()

        # Filter to requested state
        state_gdf = gdf[gdf["name"] == normalized_state]

        if state_gdf.empty:
            raise ValueError(f"State '{state}' not found in geometry data")

        return state_gdf


class GeotiffProcessor:
    """Class for processing and saving geotiff files."""

    @staticmethod
    def process_and_upload_geotiff(task: GeotiffTask) -> Tuple[bool, str]:
        """Save a dataarray as a geotiff file and upload it to S3

        Args:
            task (GeotiffTask): Task containing data array and output info

        Returns:
            Tuple[bool, str]: Success status and file name
        """

        try:
            task.da.rio.to_raster(str(task.output_path), driver=task.geotiff_driver)

            s3 = S3Handler(bucket_name=task.s3_bucket)
            upload_success = s3.upload_file(
                local_path=task.output_path, s3_key_prefix=task.s3_prefix
            )

            if upload_success:
                return True, task.output_path.name
            else:
                logger.warning(f"Saved {task.output_path.name} but failed to upload")
                return False, task.output_path.name

        except Exception as e:
            logger.error(
                f"Error processing {task.variable}-{task.decade_month}: {str(e)}"
            )
            return False, f"{task.variable}-{task.decade_month}"

    @staticmethod
    def prepare_tasks(
        ds: xr.Dataset,
        geometry: gpd.GeoDataFrame,
        state_name: str,
        output_dir: Path,
        s3_bucket: str,
        s3_prefix: str,
        geotiff_driver: str
    ) -> List[GeotiffTask]:
        """Process a dataset and prepare tasks for geotiff creation

        Args:
            ds (xr.Dataset): Dataset to process
            geometry (gpd.GeoDataFrame): Geodataframe containing geometries of clipping mask to use
            state_name (str): State/region name for use on output files
            output_dir (Path): Local (temp) directory to save generated geotiffs temporarily
            s3_bucket (str): S3 bucket name
            s3_prefix (str): S3 prefix for uploads

        Returns:
            List[GeotiffTask]: List of prepared geotiff tasks
        """

        tasks = []

        # Loop through each variable and timestep(ASSUME decade month, for example 2030-08, 2030-09, etc...) so that
        # we generate an individual geotiff file per variable and timestep
        for variable in ds.keys():
            for decade_month in ds["decade_month"].data:
                da = ds[variable].sel(decade_month=decade_month)

                clipped_array = da.rio.clip(
                    geometry.geometry.values, geometry.crs, drop=True, all_touched=False
                )

                file_name = f"{variable}-{decade_month}-{state_name}.tif"
                output_path = output_dir / file_name

                task = GeotiffTask(
                    da=clipped_array,
                    output_path=output_path,
                    variable=variable,
                    decade_month=str(decade_month),
                    s3_bucket=s3_bucket,
                    s3_prefix=s3_prefix,
                    geotiff_driver=geotiff_driver
                )
                tasks.append(task)
        return tasks


class MainProcessor:
    """Main class for procesing the data"""

    def __init__(
        self,
        s3_bucket: str,
        s3_uri_input: str,
        s3_prefix_geotiff: str,
        state: str,
        crs: str,
        x_dim: str,
        y_dim: str,
        geotiff_driver: str,
        max_workers: int,
    ):
        """
        Initialize climate data processor.

        Args:
            s3_bucket (str): AWS S3 bucket name
            s3_uri_input (str): S3 URI for input climate zarr data
            s3_prefix_geotiff (str): S3 prefix for output geotiffs directory
            state (str): US state to process
            crs (str, optional): Coordinate reference system. Defaults to "4326".
            x_dim (str, optional): X dimension name. Defaults to "lon".
            y_dim (str, optional): Y dimension name. Defaults to "lat".
            geotiff_driver (str, optional): Driver to use for Geotiff file format. 
            Defaults to "COG" for Cloud Optimized Geotiff
            max_workers (int, optional): Max parallel workers. Defaults to 16.
        """

        self.s3_bucket = s3_bucket
        self.s3_uri_input = s3_uri_input
        self.s3_prefix_geotiff = s3_prefix_geotiff
        self.state = state
        self.crs = crs
        self.x_dim = x_dim
        self.y_dim = y_dim
        self.geotiff_driver = geotiff_driver
        self.max_workers = max_workers

    def process(self) -> None:
        """Process climate data, generate geotiffs, and upload them to S3."""
        try:
            # Get state geometry
            logger.info(f"Fetching geometry for {self.state}")
            gdf = StateGeometryFetcher.get_state_geometry(state=self.state)

            # Load dataset
            logger.info(f"Loading dataset from {self.s3_uri_input}")
            ds = xr.load_dataset(self.s3_uri_input)

            # Set geospatial info
            logger.info("Setting coordinate reference system")
            ds = (
                ds.rio.write_crs(self.crs, inplace=True)
                .rio.set_spatial_dims(x_dim=self.x_dim, y_dim=self.y_dim, inplace=True)
                .rio.write_coordinate_system(inplace=True)
            )

            # Process in temporary directory
            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_path = Path(tmpdir)
                logger.info(f"Using temporary directory: {tmp_path}")

                # Prepare tasks
                tasks = GeotiffProcessor.prepare_tasks(
                    ds=ds,
                    geometry=gdf,
                    state_name=self.state,
                    output_dir=tmp_path,
                    s3_bucket=self.s3_bucket,
                    s3_prefix=self.s3_prefix_geotiff,
                    geotiff_driver=self.geotiff_driver
                )
                logger.info(f"Prepared {len(tasks)} geotiff tasks")

                # Process files in parallel and upload them immediately
                self._process_tasks_parallel(tasks)

            logger.info("Processing completed successfully")

        except Exception as e:
            logger.error(f"Error processing climate data: {str(e)}")
            raise

    def _process_tasks_parallel(self, tasks: List[GeotiffTask]) -> None:
        """
        Process geotiff tasks in parallel, uploading each file as it's generated.

        Args:
            tasks (List[GeotiffTask]): List of geotiff tasks to process
        """
        successful = 0
        upload_failed = 0
        processing_failed = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(GeotiffProcessor.process_and_upload_geotiff, task): task
                for task in tasks
            }

            for future in as_completed(futures):
                task = futures[future]
                try:
                    success, file_name = future.result()
                    if success:
                        successful += 1
                    else:
                        upload_failed += 1

                    # Log progress periodically
                    if (successful + upload_failed + processing_failed) % 10 == 0 or (
                        successful + upload_failed + processing_failed
                    ) == len(tasks):
                        total_processed = successful + upload_failed + processing_failed
                        logger.info(
                            f"Progress: {total_processed}/{len(tasks)} | "
                            f"Success: {successful} | Upload failed: {upload_failed} | "
                            f"Processing failed: {processing_failed}"
                        )
                except Exception as e:
                    processing_failed += 1
                    logger.error(
                        f"Error processing task {task.variable}-{task.decade_month}: {str(e)}"
                    )

        # Log final summary
        if upload_failed > 0 or processing_failed > 0:
            logger.warning(
                f"Completed with {successful} successes, "
                f"{upload_failed} upload failures, and "
                f"{processing_failed} processing failures "
                f"out of {len(tasks)} total tasks"
            )
        else:
            logger.info(
                f"All {successful} geotiffs processed and uploaded successfully"
            )


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Process climate data for a given state and upload as geotiffs"
    )

    parser.add_argument("--s3-bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--s3-uri-input", required=True, help="S3 URI for input climate zarr data"
    )
    parser.add_argument(
        "--s3-prefix-geotiff",
        required=True,
        help="S3 base prefix for outputting geotiffs",
    )
    parser.add_argument(
        "--state", required=True, help="US State output mask for geotiff"
    )
    parser.add_argument(
        "--crs",
        default="4326",
        help="Coordinate Reference System of Climate Zarr. Default: 4326"
    )
    parser.add_argument(
        "--x-dim",
        default="lon",
        help="Name of x dimension in the climate dataset. Default: lon"
    )
    parser.add_argument(
        "--y-dim",
        default="lat",
        help="Name of y dimension in the climate dataset. Default: lat"
    )
    parser.add_argument(
        "--geotiff-driver",
        default="COG",
        help="Driver to use for Geotiff file format Default: COG"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=16,
        help="Maximum number of parallel workers. Default: 16"
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    processor = MainProcessor(
        s3_bucket=args.s3_bucket,
        s3_uri_input=args.s3_uri_input,
        s3_prefix_geotiff=args.s3_prefix_geotiff,
        state=args.state,
        crs=args.crs,
        x_dim=args.x_dim,
        y_dim=args.y_dim,
        geotiff_driver=args.geotiff_driver,
        max_workers=args.max_workers,
    )
    
    processor.process()

if __name__ == "__main__":
    main()
