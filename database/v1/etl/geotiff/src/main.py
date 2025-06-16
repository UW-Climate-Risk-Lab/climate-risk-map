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
import numpy as np
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

X_DIM = "lon"
Y_DIM = "lat"

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


class RegionGeometryFetcher:
    """Class responsible for fetching and processing region geometry data"""


    @staticmethod
    def get_region_geometry(region: str) -> gpd.GeoDataFrame:

        region_file = f"regions/{region}.geojson"

        # Handle individual region
        gdf = gpd.read_file(region_file)

        if gdf.empty:
            raise ValueError(f"region '{region}' not found in geometry data")

        return gdf


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
    def resample_data_array(
        da: xr.DataArray, 
        output_resolution: float,
        resampling_method: str = "linear"
    ) -> xr.DataArray:
        """
        Resample a DataArray to a higher resolution.
        
        Args:
            da (xr.DataArray): Data array to resample
            output_resolution (float): Desired resolution in degrees
            resampling_method (str): Resampling method to use (default: linear)
            
        Returns:
            xr.DataArray: Resampled data array
        """
        if output_resolution <= 0:
            logger.warning("Output resolution must be positive, using original resolution")
            return da
            
        # Get current resolution and dimensions
        current_res_x = abs(float(da[X_DIM][1] - da[X_DIM][0]))
        current_res_y = abs(float(da[Y_DIM][1] - da[Y_DIM][0]))
        
        # Check if resampling is needed
        if abs(current_res_x - output_resolution) < 1e-6 and abs(current_res_y - output_resolution) < 1e-6:
            logger.debug("Output resolution matches current resolution, skipping resampling")
            return da
            
        # Define new coordinates
        x_min, x_max = float(da[X_DIM].min()), float(da[X_DIM].max())
        y_min, y_max = float(da[Y_DIM].min()), float(da[Y_DIM].max())
        
        # Create new coordinate arrays with the desired resolution
        new_x = np.arange(x_min, x_max + output_resolution/2, output_resolution)
        new_y = np.arange(y_min, y_max + output_resolution/2, output_resolution)
        
        # Check if using cubic interpolation with NaN values
        if resampling_method in ["cubic", "cubic_spline"] and da.isnull().any():
            logger.warning(
                f"Data contains NaN values which are incompatible with {resampling_method} interpolation. "
                f"Falling back to 'linear' interpolation."
            )
            safe_method = "linear"
        else:
            safe_method = resampling_method
        
        try:
            # Resample using xarray's interp method
            resampled = da.interp({X_DIM: new_x, Y_DIM: new_y}, method=safe_method)
            
            # Preserve metadata and attributes
            resampled.attrs.update(da.attrs)
            
            # Set CRS and spatial dimensions
            if hasattr(da, 'rio'):
                resampled.rio.write_crs(da.rio.crs, inplace=True)
                resampled.rio.set_spatial_dims(x_dim=X_DIM, y_dim=Y_DIM, inplace=True)
            
            logger.info(
                f"Resampled data from {len(da[X_DIM])}x{len(da[Y_DIM])} to {len(resampled[X_DIM])}x{len(resampled[Y_DIM])}"
            )
            
            return resampled
            
        except ValueError as e:
            # If we still get an error, fall back to nearest neighbor which always works
            logger.warning(
                f"Error during {safe_method} interpolation: {str(e)}. "
                f"Falling back to 'nearest' interpolation which can handle NaN values."
            )
            resampled = da.interp({X_DIM: new_x, Y_DIM: new_y}, method="nearest")
            
            # Preserve metadata and attributes
            resampled.attrs.update(da.attrs)
            
            # Set CRS and spatial dimensions
            if hasattr(da, 'rio'):
                resampled.rio.write_crs(da.rio.crs, inplace=True)
                resampled.rio.set_spatial_dims(x_dim=X_DIM, y_dim=Y_DIM, inplace=True)
                
            logger.info(
                f"Resampled data from {len(da[X_DIM])}x{len(da[Y_DIM])} to {len(resampled[X_DIM])}x{len(resampled[Y_DIM])}"
            )
            
            return resampled

    @staticmethod
    def prepare_tasks(
        ds: xr.Dataset,
        geometry: gpd.GeoDataFrame | None,
        variable: str,
        region_name: str,
        output_dir: Path,
        s3_bucket: str,
        s3_prefix: str,
        geotiff_driver: str,
        output_resolution: float = None,
        resampling_method: str = "linear"
    ) -> List[GeotiffTask]:
        """Process a dataset and prepare tasks for geotiff creation

        Args:
            ds (xr.Dataset): Dataset to process
            geometry (gpd.GeoDataFrame): Geodataframe containing geometries of clipping mask to use
            variable (str): Variable to use in Dataset
            region_name (str): Region name for use in output file names
            output_dir (Path): Local (temp) directory to save generated geotiffs temporarily
            s3_bucket (str): S3 bucket name
            s3_prefix (str): S3 prefix for uploads
            geotiff_driver (str): Driver to use for Geotiff file format
            output_resolution (float, optional): Desired output resolution in degrees. If None, 
                                                 original resolution is preserved.
            resampling_method (str, optional): Method to use for resampling. Default is "linear".

        Notes:
            The function supports two different temporal structures in the input dataset:
            1. Legacy datasets containing a single "decade_month" dimension (e.g. "2050-06").
            2. Newer datasets that contain both "year_period" (e.g. "2015-2044") and "return_period" 
               (e.g. 2, 5, 100) dimensions. In this case, a separate GeoTIFF is created for every
               combination of year_period and return_period.

        Returns:
            List[GeotiffTask]: List of prepared geotiff tasks
        """

        tasks = []
        # Converts 0-360 longitude to -180-180 longitude. In line with OpenStreetMap database and geojson region files
        ds = ds.assign_coords({X_DIM: (((ds[X_DIM] + 180) % 360) - 180)})
        ds = ds.sortby(X_DIM)

        # Determine which temporal dimensions are available and iterate accordingly
        if "decade_month" in ds.dims:
            # Legacy behaviour – iterate over decade_month
            for decade_month in ds["decade_month"].data:
                da = ds[variable].sel(decade_month=decade_month)
                da.rio.set_spatial_dims(x_dim=X_DIM, y_dim=Y_DIM, inplace=True)

                # Apply clipping if a geometry is provided
                if geometry:
                    clipped_array = da.rio.clip(
                        geometry.geometry.values, geometry.crs, drop=True, all_touched=True
                    )
                    file_name = f"{variable}-{decade_month}-{region_name}.tif"
                else:
                    clipped_array = da
                    file_name = f"{variable}-{decade_month}-global.tif"
                
                # Apply resampling if output_resolution is specified
                if output_resolution is not None:
                    resampled_array = GeotiffProcessor.resample_data_array(
                        clipped_array, 
                        output_resolution,
                        resampling_method
                    )
                    res_str = str(output_resolution).replace('.', 'p')
                    if "global" in file_name:
                        file_name = f"{variable}-{decade_month}-global-{res_str}deg.tif"
                    else:
                        file_name = f"{variable}-{decade_month}-{region_name}-{res_str}deg.tif"
                    clipped_array = resampled_array
                
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

        elif {"year_period", "return_period"}.issubset(ds.dims):
            # New dataset type – iterate over all combinations. month_of_year may or may not be present.
            has_month = "month_of_year" in ds.dims

            month_values = (
                ds["month_of_year"].data if has_month else [None]
            )

            for year_period in ds["year_period"].data:
                for return_period in ds["return_period"].data:
                    for month in month_values:
                        selection_kwargs = dict(
                            year_period=year_period,
                            return_period=return_period,
                        )
                        if month is not None:
                            selection_kwargs["month_of_year"] = month

                        da = ds[variable].sel(selection_kwargs)
                        da.rio.set_spatial_dims(x_dim=X_DIM, y_dim=Y_DIM, inplace=True)

                        # Apply clipping if a geometry is provided
                        if geometry:
                            clipped_array = da.rio.clip(
                                geometry.geometry.values,
                                geometry.crs,
                                drop=True,
                                all_touched=True,
                            )
                        else:
                            clipped_array = da

                        # Build file name parts
                        if month is not None:
                            time_str = f"{year_period}-{int(month)}-{return_period}"
                        else:
                            time_str = f"{year_period}-{return_period}"

                        if geometry:
                            file_name = f"{variable}-{time_str}-{region_name}.tif"
                        else:
                            file_name = f"{variable}-{time_str}-global.tif"

                        # Apply resampling if output_resolution is specified
                        if output_resolution is not None:
                            resampled_array = GeotiffProcessor.resample_data_array(
                                clipped_array,
                                output_resolution,
                                resampling_method,
                            )
                            res_str = str(output_resolution).replace(".", "p")
                            if "global" in file_name:
                                file_name = (
                                    f"{variable}-{time_str}-global-{res_str}deg.tif"
                                )
                            else:
                                file_name = (
                                    f"{variable}-{time_str}-{region_name}-{res_str}deg.tif"
                                )
                            clipped_array = resampled_array

                        output_path = output_dir / file_name

                        task = GeotiffTask(
                            da=clipped_array,
                            output_path=output_path,
                            variable=variable,
                            decade_month=str(time_str),  # reuse field for general time id
                            s3_bucket=s3_bucket,
                            s3_prefix=s3_prefix,
                            geotiff_driver=geotiff_driver,
                        )
                        tasks.append(task)

        else:
            raise ValueError(
                "Dataset does not contain the expected temporal dimensions: "
                "either 'decade_month' or the combination of 'year_period' and 'return_period'."
            )
        return tasks


class MainProcessor:
    """Main class for procesing the data"""

    def __init__(
        self,
        s3_bucket: str,
        s3_uri_input: str,
        s3_prefix_geotiff: str,
        region: str,
        crs: str,
        variable: str,
        x_dim: str,
        y_dim: str,
        geotiff_driver: str,
        max_workers: int,
        output_resolution: Optional[float] = None,
        resampling_method: str = "cubic"
    ):
        """
        Initialize climate data processor.

        Args:
            s3_bucket (str): AWS S3 bucket name
            s3_uri_input (str): S3 URI for input climate zarr data
            s3_prefix_geotiff (str): S3 prefix for output geotiffs directory
            region (str): US region to process
            crs (str, optional): Coordinate reference system. Defaults to "4326".
            variable (str, optional): Variable of Dataset to use. Defaults to "ensemble_q3"
            x_dim (str, optional): X dimension name. Defaults to "lon".
            y_dim (str, optional): Y dimension name. Defaults to "lat".
            geotiff_driver (str, optional): Driver to use for Geotiff file format. 
            Defaults to "COG" for Cloud Optimized Geotiff
            max_workers (int, optional): Max parallel workers. Defaults to 16.
            output_resolution (float, optional): Desired output resolution in degrees. If None, 
                                                original resolution is preserved.
            resampling_method (str, optional): Method to use for resampling. Default is "cubic".
        """

        self.s3_bucket = s3_bucket
        self.s3_uri_input = s3_uri_input
        self.s3_prefix_geotiff = s3_prefix_geotiff
        self.region = region
        self.crs = crs
        self.variable = variable
        self.x_dim = x_dim
        self.y_dim = y_dim
        self.geotiff_driver = geotiff_driver
        self.max_workers = max_workers
        self.output_resolution = output_resolution
        self.resampling_method = resampling_method

    def process(self) -> None:
        """Process climate data, generate geotiffs, and upload them to S3."""
        try:

            s3_prefix_geotiff = f"{self.s3_prefix_geotiff}/{self.region}"

            # Get region geometry
            logger.info(f"Fetching geometry for {self.region}")
            if self.region == "global":
                gdf = None
            else:
                gdf = RegionGeometryFetcher.get_region_geometry(region=self.region)

            # Load dataset
            logger.info(f"Loading dataset from {self.s3_uri_input}")
            ds = xr.open_zarr(self.s3_uri_input)

            if {"year_period", "return_period"}.issubset(ds.dims):
                # Remove unnecessary coordinate variables that can conflict with dimension names
                ds = ds.drop_vars(["start_year", "end_year"], errors="ignore")

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
                    region_name=self.region,
                    output_dir=tmp_path,
                    variable=self.variable,
                    s3_bucket=self.s3_bucket,
                    s3_prefix=s3_prefix_geotiff,
                    geotiff_driver=self.geotiff_driver,
                    output_resolution=self.output_resolution,
                    resampling_method=self.resampling_method
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
        description="Process climate data for a given region and upload as geotiffs"
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
        "--region", required=True, help="Region output mask for geotiff"
    )
    parser.add_argument(
        "--crs",
        default="4326",
        help="Coordinate Reference System of Climate Zarr. Default: 4326"
    )
    parser.add_argument(
        "--variable",
        default="ensemble_q3",
        help="Variable in dataset to to use to get Dataarray"
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
    parser.add_argument(
        "--output-resolution",
        type=float,
        default=None, 
        help="Desired output resolution in degrees (e.g., 0.1 for higher resolution). "
             "If not specified, original resolution is preserved."
    )
    parser.add_argument(
        "--resampling-method",
        default="linear",
        choices=["nearest", "linear", "cubic"],
        help="Method to use for resampling. Default: cubic"
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    processor = MainProcessor(
        s3_bucket=args.s3_bucket,
        s3_uri_input=args.s3_uri_input,
        s3_prefix_geotiff=args.s3_prefix_geotiff,
        region=args.region,
        crs=args.crs,
        variable=args.variable,
        x_dim=args.x_dim,
        y_dim=args.y_dim,
        geotiff_driver=args.geotiff_driver,
        max_workers=args.max_workers,
        output_resolution=args.output_resolution,
        resampling_method=args.resampling_method,
    )
    
    processor.process()

if __name__ == "__main__":
    main()