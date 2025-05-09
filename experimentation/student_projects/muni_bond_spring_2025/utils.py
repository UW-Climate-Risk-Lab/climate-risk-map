import pandas as pd
import xarray as xr
import requests # For downloading files
import os       # For path operations
from typing import List, Optional

def _preprocess_add_time_from_filename(ds: xr.Dataset) -> xr.Dataset:
    """
    Preprocessing function for xarray.open_mfdataset.
    Extracts date from filename and assigns it as the time coordinate.
    Assumes filename format like: FWI.GPM.LATE.v5.Daily.Default.YYYYMMDD.nc
    """
    try:
        filename = os.path.basename(ds.encoding['source'])
        # Extract YYYYMMDD part. Example: FWI.GPM.LATE.v5.Daily.Default.20230315.nc
        # Parts: ['FWI', 'GPM', 'LATE', 'v5', 'Daily', 'Default', '20230315', 'nc']
        date_str = filename.split('.')[-2] 
        
        if len(date_str) == 8 and date_str.isdigit():
            correct_time = pd.to_datetime(date_str, format='%Y%m%d')
            # Assign this timestamp to the 'time' coordinate.
            # This assumes 'time' is a dimension of size 1 in the original file.
            ds = ds.assign_coords(time=[correct_time])
        else:
            print(f"Warning: Could not parse date from filename: {filename}. Time coordinate will not be set for this file.")
    except Exception as e:
        # Catch any other errors during preprocessing for a specific file
        filename_for_error = "unknown (ds.encoding not available)"
        if 'source' in ds.encoding:
            filename_for_error = ds.encoding['source']
        print(f"Warning: Error preprocessing file {filename_for_error} to add time coordinate: {e}")
    return ds

def download_and_read_fwi_data_for_dates(
    dates_series: pd.Series, 
    local_data_dir: str = "data/nccs_fwi_data"
) -> Optional[xr.Dataset]:
    """
    Downloads NetCDF files from the NASA NCCS portal for a given series of dates
    to a local directory (if they don't already exist), then reads them 
    into a single xarray Dataset, adding a proper time coordinate during load.

    Example file path structure:
    https://portal.nccs.nasa.gov/datashare/GlobalFWI/v2.0/fwiCalcs.GEOS-5/Default/GPM.LATE.v5/{YYYY}/FWI.GPM.LATE.v5.Daily.Default.{YYYYMMDD}.nc

    Args:
        dates_series (pd.Series): A pandas Series containing datetime64 objects.
                                  Each datetime represents a day for which data
                                  should be fetched.
        local_data_dir (str): The local directory path where NetCDF files will be
                              downloaded or are expected to be. It will be 
                              created if it doesn't exist.

    Returns:
        Optional[xr.Dataset]: An xarray Dataset containing the combined data from 
                              all specified dates. Returns None if the input series 
                              is empty, no valid URLs are generated, or if
                              downloading/reading fails for all files.
                              
    Raises:
        ImportError: If pandas, xarray, or requests is not installed.
        Any exceptions raised by xarray.open_mfdataset if files are corrupted
        (e.g., OSError), or if preprocessing fails critically.
    """
    if not isinstance(dates_series, pd.Series):
        raise TypeError("Input 'dates_series' must be a pandas Series.")
    if dates_series.empty:
        print("Input dates_series is empty. Returning None.")
        return None

    # Ensure the local data directory exists
    try:
        os.makedirs(local_data_dir, exist_ok=True)
        print(f"Data will be handled in/downloaded to: {os.path.abspath(local_data_dir)}")
    except OSError as e:
        print(f"Error creating directory {local_data_dir}: {e}")
        return None

    # Base URL for the data portal
    base_url_template = "https://portal.nccs.nasa.gov/datashare/GlobalFWI/v2.0/fwiCalcs.GEOS-5/Default/GPM.LATE.v5/{year}/"
    # Filename template
    filename_template = "FWI.GPM.LATE.v5.Daily.Default.{date_str}.nc"

    files_to_process_locally: List[str] = [] # Stores paths of files to be opened by xarray

    for dt in dates_series:
        if not hasattr(dt, 'year') or not hasattr(dt, 'strftime'):
            try:
                dt = pd.to_datetime(dt)
            except ValueError as e:
                print(f"Warning: Could not convert '{dt}' to datetime: {e}. Skipping this entry.")
                continue
        
        year = dt.year
        date_str = dt.strftime('%Y%m%d') 

        file_name = filename_template.format(date_str=date_str)
        local_target_path = os.path.join(local_data_dir, file_name)

        # Check if the file already exists locally
        if os.path.exists(local_target_path):
            print(f"File '{file_name}' already exists locally at '{local_target_path}'. Using existing file.")
            files_to_process_locally.append(local_target_path)
            continue # Skip download for this file

        # If file doesn't exist, proceed to download
        specific_base_url = base_url_template.format(year=year)
        full_url = specific_base_url + file_name
        
        print(f"Attempting to download {full_url} to {local_target_path}...")
        try:
            response = requests.get(url=full_url, timeout=60) 
            response.raise_for_status() 
            
            with open(local_target_path, 'wb') as f:
                f.write(response.content) 
            print(f"Successfully downloaded {file_name}")
            files_to_process_locally.append(local_target_path)
        except requests.exceptions.HTTPError as e:
            print(f"HTTPError downloading {full_url}: {e}")
        except requests.exceptions.ConnectionError as e:
            print(f"ConnectionError downloading {full_url}: {e}")
        except requests.exceptions.Timeout as e:
            print(f"Timeout downloading {full_url}: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {full_url}: {e}")
        except IOError as e:
            print(f"IOError saving file {local_target_path}: {e}")
        # If a download fails, we just print the error and continue to the next file.

    if not files_to_process_locally:
        print("No files available for processing (either not found, not downloadable, or none specified). Returning None.")
        return None

    print(f"\nAttempting to open {len(files_to_process_locally)} files with xarray, applying time preprocessing:")

    try:
        # Use xarray.open_mfdataset with the preprocess function
        combined_ds = xr.open_mfdataset(
            files_to_process_locally, 
            combine='by_coords', # Should now work with corrected time coordinates
            preprocess=_preprocess_add_time_from_filename, # Apply our function
            engine='netcdf4' 
        )
        # It's good practice to sort by time if the order of files_to_process_locally
        # wasn't guaranteed or if dates_series wasn't sorted.
        if 'time' in combined_ds.coords:
             combined_ds = combined_ds.sortby('time')
        return combined_ds
    except Exception as e:
        print(f"An error occurred while opening or combining local NetCDF files with xarray: {e}")
        print("Please ensure files are valid NetCDF format and preprocessing logic is correct for filenames.")
        print("You might also need to install/check a NetCDF engine: pip install netcdf4 h5netcdf")
        return None

if __name__ == '__main__':
    # --- Example Usage ---

    data_directory = "my_fwi_data_downloads_v3" # Changed for fresh test

    historical_dates_list = [
        '2023-03-17', # Mix order to test sortby
        '2023-03-15',
        '2023-03-16', 
        '2023-03-18', 
    ]
    my_dates_series = pd.Series(pd.to_datetime(historical_dates_list))

    print("--- Downloading and Reading FWI Data (with time preprocessing) ---")
    print("Dates to process/fetch:")
    print(my_dates_series) # Note: series might not be sorted here
    print(f"Local directory for data: ./{data_directory}") 
    print("-" * 40)
    
    fwi_dataset = download_and_read_fwi_data_for_dates(
        my_dates_series, 
        local_data_dir=data_directory
    )

    if fwi_dataset:
        print("\n--- Successfully loaded dataset ---")
        print(fwi_dataset)
        
        if 'time' in fwi_dataset.coords:
            print("\nTime coordinate values:")
            print(fwi_dataset['time'].values)
            # Verify if it's sorted
            if pd.Series(fwi_dataset['time'].values).is_monotonic_increasing:
                print("Time coordinate is sorted.")
            else:
                print("Warning: Time coordinate is NOT sorted.")

        if 'FWI' in fwi_dataset.variables: # Assuming FWI is a variable
            print("\nFWI variable info (first time step, sample region):")
            # print(fwi_dataset['FWI'].isel(time=0, lat=slice(0,5), lon=slice(0,5)))
            print(fwi_dataset['FWI'])
        else:
            print("\n'FWI' variable not found or dataset structure unexpected. Available variables:")
            print(list(fwi_dataset.variables))
            print("\nAvailable coordinates:")
            print(list(fwi_dataset.coords))
    else:
        print("\n--- Failed to load dataset or no data was successfully processed ---")
        print(f"Check the '{data_directory}' for any downloaded files and review error messages above.")

    print("\n--- Important Notes ---")
    print("1. Requires an active internet connection for new downloads.")
    print("2. Libraries needed: `pandas`, `xarray`, `requests` (`pip install pandas xarray requests`).")
    print("3. NetCDF engine: `netcdf4` or `h5netcdf` (`pip install netcdf4 h5netcdf`).")
    print("4. Ensure the dates correspond to available files on the NCCS portal for new downloads.")
    print(f"5. Files are stored/checked in the '{data_directory}' directory.")
    print("6. Time coordinate is now added based on filename during load.")
