# Precipitation Frequency Analysis for Climate Impact Assessment

## 1. Objective

This project aims to calculate historical and future month-specific Precipitation Frequency Estimates (PFEs) and Pluvial Change Factors (PCFs) using daily precipitation data from the NASA NEX GDDP CMIP6 climate model ensemble. The primary goal is to generate robust indicators of extreme rainfall characteristics, which are fundamental drivers of pluvial (rainfall-induced) flood risk, with a specific focus on understanding and quantifying seasonal variations in these extremes.

The methodology draws inspiration from concepts outlined in the "First Street Flood Methodology, Version 3.0" document, such as the use of PFEs, PCFs, the Generalized Extreme Value (GEV) distribution, and standard return periods. A key aspect of this project is its **month-specific analysis** approach. This provides a granular, seasonal perspective on precipitation extremes, which can be critical for regional risk assessment and adaptation planning where the timing of extreme events within the year is important.

The Pluvial Change Factor (PCF) calculated in this project is **return-period-specific**, offering detailed insight into how the ratio of future to historical extreme precipitation varies across different event magnitudes (e.g., for 2-year events versus 100-year events). This differs from a single change factor approach that might apply uniformly across all return periods.

The outputs are designed for efficient computation and storage, with historical PFEs cached to facilitate the calculation of future changes across various scenarios and time periods.

## 2. Methodology

The analysis involves processing daily precipitation data from selected CMIP6 GCMs for a defined historical baseline period and one or more future periods under specified Shared Socioeconomic Pathways (SSPs). All calculations are performed for each GCM ensemble member independently.

### 2.1. Data and Tools

* **Input Data:**
    * Daily precipitation data (`pr`) from the NASA NEX GDDP CMIP6 collection, sourced from AWS S3. File discovery is handled by the `find_best_file` utility.
    * **Historical Baseline Period:** A defined historical period (e.g., 1981-2014 by default in the script, consistent with CMIP6 historical runs).
    * **Future Periods & Scenarios:** User-configurable future time windows (e.g., decadal spans like 2030-2039, 2040-2049, up to `LAST_FUTURE_YEAR`) and SSP scenarios (e.g., SSP2-4.5, SSP5-85, as supported by the GCMs in `models.py`). The First Street methodology primarily uses SSP2-4.5.
* **Core Libraries:**
    * `Python`: Main programming language.
    * `xarray`: For multi-dimensional data handling.
    * `xclim`: For climate indices and frequency analysis, particularly `xclim.indices.stats.fa`.
    * `dask` & `dask.distributed`: For parallel processing of large datasets.
    * `s3fs`, `boto3`: For interaction with AWS S3.
* **Output:**
    * Zarr stores on AWS S3.
    * **Historical Data:** One Zarr store per GCM ensemble member, containing historical month-specific PFEs (variable named `historical_pfe`). Path example: `s3://{S3_BUCKET}/{OUTPUT_ZARR_PATH_PREFIX}/{model}/historical/{ensemble_member}/pr_pfe_month_of_year_{model}_historical_{ensemble_member}_gn_{hist_start}-{hist_end}.zarr`
    * **Future Data:** One Zarr store per GCM ensemble member, per future period, and per SSP scenario. This store contains:
        * Future month-specific PFEs (variable named `future_monthly_pfe_mm_day`).
        * Month-specific and return-period-specific Pluvial Change Factors (variable named `pluvial_change_factor`).
        Path example: `s3://{S3_BUCKET}/{OUTPUT_ZARR_PATH_PREFIX}/{model}/{scenario}/{ensemble_member}/pr_pfe_pcf_month_of_year_{model}_{scenario}_{ensemble_member}_gn_{future_start}-{future_end}.zarr`

### 2.2. Calculation Steps

The script (`pfe_calc.py`) orchestrates the workflow, which can be broken down as follows:

**A. Calculation of Historical Month-Specific PFEs (Function: `process_historical`)**

This is performed once per GCM ensemble member for the defined historical baseline period.

1.  **Data Acquisition and Preparation (within `_calculate_monthly_pfes_for_period`):**
    * Load daily precipitation (`pr`) data for the specified GCM, "historical" scenario, ensemble member, and historical baseline years (e.g., 1981-2014).
    * Convert units from kg m<sup>-2</sup> s<sup>-1</sup> to mm/day.
    * Apply Dask chunking as per `CHUNKS_CONFIG` (e.g., `{'time': -1, 'lat': 120, 'lon': 288}`) and persist (`pr_hist.persist()`) the daily data for the full period.
2.  **Generation of Annual Series of Monthly Maxima & Frequency Analysis (within `calculate_monthly_return_periods_for_apply`, called via `groupby('time.month').apply`):**
    * For each of the 12 calendar months:
        * The function receives the subset of daily precipitation data corresponding to that specific month across all years in the historical baseline.
        * It computes an "annual series of monthly maxima": for each year, it finds the maximum daily precipitation value that occurred *within that specific month* (`annual_max_for_month = pr_daily_data_for_one_month.groupby('time.year').max(dim='time', skipna=True)`).
        * The 'year' dimension of this series is renamed to 'time', and the 'time' coordinate values are converted to `numpy.datetime64` objects (e.g., 'YYYY-07-01') for `xclim` compatibility.
        * The 'time' dimension of `annual_max_for_month_renamed` is rechunked to `{'time': -1}` to ensure it's a single Dask chunk before being passed to `xclim.indices.stats.fa`.
        * The `xclim.indices.stats.fa` function is applied to this series to calculate PFEs:
            * `t`: Desired return periods (e.g., `[2, 5, 20, 100, 500]` years).
            * `dist='genextreme'`: Uses the Generalized Extreme Value (GEV) distribution.
            * `mode='max'`: For analyzing maximum precipitation values.
        * The function is designed to return a consistently shaped `xarray.DataArray` (with dimensions `return_period`, `lat`, `lon`) filled with NaNs if errors occur or data is insufficient, using template coordinates passed to it. This is crucial for preventing issues during the `groupby().apply()` concatenation.
3.  **Aggregation and Storage of Historical PFEs:**
    * The results from the 12 monthly frequency analyses are combined by `groupby().apply()` into a single `xarray.DataArray` (`historical_pfe`) with dimensions `(month_of_year, return_period, lat, lon)`.
    * This DataArray is saved as the variable "historical\_pfe" within a Dataset to a dedicated Zarr store on S3. This Zarr is cached for use in future scenario processing.

**B. Calculation of Future Month-Specific PFEs and PCFs (Function: `process_future_scenario`)**

This is performed for each GCM ensemble member, for each specified future time period and SSP scenario.

1.  **Load Cached Historical PFEs:**
    * The full historical month-specific PFE DataArray (`historical_pfe` from step A3) is loaded from its Zarr store on S3 using the `load_historical_pfe_from_s3` function. This data contains PFEs for all return periods.
2.  **Calculate Future Month-Specific PFEs:**
    * For the specified future period (e.g., 2030-2039) and SSP scenario (e.g., "ssp585"):
        * The `_calculate_monthly_pfes_for_period` function (same core logic as in A1-A2) is called to load future daily precipitation data and compute the `future_monthly_pfes_da` using `calculate_monthly_return_periods_for_apply`. This also results in a DataArray with dimensions `(month_of_year, return_period, lat, lon)`.
3.  **Calculate Return-Period-Specific Pluvial Change Factors (PCFs):**
    * The PCF is calculated as the element-wise ratio of the full future PFE array to the full historical PFE array:
        `pcf = future_monthly_pfes_da / historical_pfe`
    * The resulting `pcf` DataArray will therefore also have dimensions `(month_of_year, return_period, lat, lon)`, making it a **return-period-specific PCF**. This means there will be a different PCF for a 2-year event compared to a 100-year event, for each month and grid cell. This differs from the First Street methodology where PCF is typically based on the 2-year return period as a general scaling index.
4.  **Aggregation and Output Storage for Future Period:**
    * A new xarray Dataset is created for the current GCM, future period, and scenario.
    * This Dataset contains the following primary data variables:
        * `future_monthly_pfe_mm_day`: The future PFEs (`future_monthly_pfes_da`).
        * `pluvial_change_factor`: The return-period-specific PCFs (`pcf`).
    * Comprehensive metadata is added, including descriptions, GCM details, scenario, historical and future periods analyzed.
    * This Dataset is saved to a dedicated Zarr store on S3.

### 2.3. Interpretation of Key Outputs

* **Month-Specific PFE (Historical or Future):** A PFE value for a given month, return period, and location (e.g., 100-year PFE for July) represents the daily rainfall amount (in mm/day) that has an estimated 1% chance of being equaled or exceeded in any given July at that location, according to the GEV distribution fitted to the relevant dataset (historical or future) for July.
* **Month-Specific, Return-Period-Specific PCF:** A PCF value for a given month, return period, and location (e.g., 100-year PCF for July) indicates the projected change in the 100-year return period daily rainfall for July in a future period, relative to the 100-year return period daily rainfall for July in the historical baseline. A PCF > 1 suggests an intensification of that specific magnitude of extreme event for that month.

## 3. Technical Implementation

* **Script:** `pfe_calc.py`
* **Execution:** The script is driven by command-line arguments to specify the model, scenario, and future period length. It can process a single model or loop through a predefined list of GCMs (`MODELS` from `models.py`).
* **Dask for Parallelism:** Utilizes `dask.distributed.Client` for managing parallel computations. Configuration (number of workers, threads, memory) is determined from environment variables or defaults. `threads_per_worker` is typically set to 1 for CPU-bound tasks.
* **Chunking Strategy:**
    * Initial daily data is loaded with user-defined spatial chunks (e.g., `lat: 120, lon: 288`) and the full time dimension (`time: -1`), then persisted.
    * The intermediate "annual series of monthly maxima" has its 'time' (yearly) dimension rechunked to `{'time': -1}` before GEV fitting via `xclim.stats.fa` to meet Dask's core dimension requirements.
    * Final output Zarr stores have defined chunking for efficient storage and access (e.g., all months and return periods as single chunks, spatial dimensions as per initial config).
* **Error Handling & Robustness:** The `calculate_monthly_return_periods_for_apply` function ensures consistently dimensioned NaN outputs for error cases or insufficient data, which is critical for the stability of the `groupby().apply()` concatenation stage. The script also includes checks for file existence and S3 write operations.
* **Caching of Historical Data:** Historical PFE calculations are performed once per model and saved. Future scenario processing loads these cached historical PFEs, avoiding redundant computations.

## 4. Limitations of this Specific Methodology

* **Statistical Robustness with Short Future Periods:** If short time windows (e.g., 10 years for decadal future periods, as configurable by `--future-year-period`) are used for GEV fitting, the resulting future PFEs and consequently the PCFs will have higher statistical uncertainty. Longer periods (20-30 years) are generally preferred for more stable extreme value statistics.
* **Daily Data Focus:** Analysis is based on daily mean precipitation. PFEs represent 24-hour rainfall extremes and do not capture sub-daily intensities, which can be critical for certain types of flash floods. (The First Street Precipitation Model itself does consider sub-daily durations).
* **At-Site Analysis:** Frequency analysis is performed independently for each grid cell ("at-site").
* **GCM Inherent Uncertainties:** Results are subject to the inherent uncertainties associated with GCMs (model structure, internal variability) and the choice of SSP scenario.
* **PCF Interpretation:** The PCF calculated here is return-period-specific. This provides detailed insight but means there isn't a single change factor for all event magnitudes for a given month, differing from some conventional PCF approaches (like First Street's which uses the 2-year event as a general index).
* **Exclusion of Specialized TC Modeling for PCF:** This script's PCF is based purely on GCM-gridded precipitation. It does not explicitly incorporate specialized models for tropical cyclone (TC) rainfall intensity changes, which the First Street methodology includes for relevant regions in their PCF derivation.


## 5. How to Run

1. Install the UV python package manager (if not already installed). This is a common and robust package manager.
2. Ensure you are currently in this directory (`/climate-risk-map/data_processing/hazards/flood/precip/nasa_nex`)
3. Run `uv sync` in the terminal
4. Run `uv run src/main.py --scenario historical` for generating historical data