import logging
import re
import os
from pathlib import PurePosixPath
from typing import List, Dict, Tuple

import xarray as xr
import s3fs
import dask
from dask.distributed import Client
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress botocore info logs
logging.getLogger("botocore.httpchecksum").setLevel(logging.WARNING)


def extract_year_range_from_uri(uri: str) -> Tuple[int, int]:
    """Extract start and end years from URI path"""
    # Pattern to match year ranges like "2045-2074"
    year_match = re.search(r'_(\d{4})-(\d{4})\.zarr$', uri)
    if year_match:
        start_year = int(year_match.group(1))
        end_year = int(year_match.group(2))
        return start_year, end_year
    else:
        raise ValueError(f"Could not extract year range from URI: {uri}")


def extract_model_info_from_uri(uri: str) -> Dict[str, str]:
    """Extract model name, scenario, and ensemble member from URI"""
    # Pattern: .../MODEL_NAME/SCENARIO/ENSEMBLE_MEMBER/...
    parts = uri.split('/')
    
    # Find NEX-GDDP-CMIP6 index and extract info relative to it
    try:
        cmip6_idx = parts.index('NEX-GDDP-CMIP6')
        model_name = parts[cmip6_idx + 1]
        scenario = parts[cmip6_idx + 2]
        ensemble_member = parts[cmip6_idx + 3]
        
        return {
            'model': model_name,
            'scenario': scenario,
            'ensemble_member': ensemble_member
        }
    except (ValueError, IndexError) as e:
        raise ValueError(f"Could not extract model info from URI: {uri}") from e


def load_and_label_dataarray(uri: str, data_var: str) -> xr.DataArray:
    """Load a single dataarray and add start_year/end_year dimensions"""
    # Extract metadata from URI
    start_year, end_year = extract_year_range_from_uri(uri)
    model_info = extract_model_info_from_uri(uri)
    
    # Load dataset
    ds = xr.open_zarr(uri, consolidated=True)

    if data_var not in ds:
        raise KeyError(f"Variable '{data_var}' not found in {uri}")

    # Only load specified variables if provided
    da = ds[data_var]

    
    if 'model' not in da.dims:
        da = da.expand_dims(model=[model_info['model']])
    if 'start_year' not in da.dims:
        da = da.expand_dims(start_year=[start_year])
    if 'end_year' not in da.dims:
        da = da.expand_dims(end_year=[end_year])
    
    # Add attributes
    da.attrs.update({
        'model_name': model_info['model'],
        'scenario': model_info['scenario'],
        'ensemble_member': model_info['ensemble_member'],
        'start_year': start_year,
        'end_year': end_year,
        'period': f"{start_year}-{end_year}"
    })
    
    return da


def compute_ensemble_stats(da: xr.DataArray) -> xr.Dataset:
    """
    Compute statistical metrics across the 'model' dimension
    
    Args:
        da: DataArray with 'model' dimension
        
    Returns:
        Dataset containing ensemble statistics
    """
    # Ensure model dimension is not chunked for statistical operations
    if 'model' in da.dims and hasattr(da.data, 'chunks'):
        da = da.chunk({'model': -1})
    
    # Compute statistics
    stats_dict = {
        'ensemble_mean': da.mean(dim='model'),
        'ensemble_median': da.median(dim='model'),
        'ensemble_stddev': da.std(dim='model'),
        'ensemble_min': da.min(dim='model'),
        'ensemble_max': da.max(dim='model'),
        'ensemble_q1': da.quantile(0.25, dim='model').drop_vars('quantile'),
        'ensemble_q3': da.quantile(0.75, dim='model').drop_vars('quantile'),
        'ensemble_count': da.count(dim='model')  # Number of models per period
    }
    
    # Create dataset
    stats_ds = xr.Dataset(stats_dict)
    
    # Add metadata about the total number of models
    stats_ds.attrs['total_models'] = len(da.model)
    stats_ds.attrs['models_included'] = list(da.model.values)
    
    return stats_ds


def run(
    model_uris: List[str],
    scenario: str,
    output_bucket: str,
    variable_names: List[str] = ['future_monthly_pfe_mm_day', 'pluvial_change_factor'],
    output_prefix: str = 'climate-risk-map/backend/climate/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE',
    
) -> Dict[str, str]:
    """
    Run ensemble calculation for precipitation extremes
    
    Args:
        model_uris: List of S3 URIs to processed model outputs
        scenario: Scenario name (e.g., 'ssp245')
        variable_names: List of variables to process
        output_bucket: S3 bucket for output (if None, extracted from first URI)
        output_prefix: S3 prefix for output
        
    Returns:
        Dictionary mapping variable names to output URIs
    """
    if not model_uris:
        logger.warning(f"No model URIs provided for scenario {scenario}")
        return {}
    
    logger.info(f"Starting ensemble calculation for {scenario} with {len(model_uris)} models")
    
    # Collect unique year periods
    year_periods = set()
    for uri in model_uris:
        start_year, end_year = extract_year_range_from_uri(uri)
        year_periods.add((start_year, end_year))
    logger.info(f"Found {len(year_periods)} unique year periods: {sorted(year_periods)}")
    
    output_uris = {}
    model_names = list(set(extract_model_info_from_uri(uri)['model'] for uri in model_uris))

    # Process each variable
    for var_name in variable_names:
        logger.info(f"Processing variable: {var_name}")
        
        periods_dict = {}  # {(start_year, end_year): [da_model1, da_model2, ...]}

        # Group by period during loading
        for uri in model_uris:
            try:
                da = load_and_label_dataarray(uri, var_name)
                da_persisted = da.persist()
                start_yr = int(da.start_year.values[0])
                end_yr = int(da.end_year.values[0])
                period_key = (start_yr, end_yr)
                
                if period_key not in periods_dict:
                    periods_dict[period_key] = []
                periods_dict[period_key].append(da_persisted)
            except Exception as e:
                logger.error(f"Error loading {var_name} from {uri}: {e}")
                continue  # Skip this URI but continue with others
        
        
        logger.info(f"Processing {len(periods_dict.keys())} time periods")
        
        # Process each period separately and then combine
        period_stats = []
        for period_key, model_das in periods_dict.items():
            # Combine DataArrays of all models for this period
            start_yr, end_yr = period_key
            if not model_das:  # Check for empty list
                logger.warning(f"No models found for period {start_yr}-{end_yr}")
                continue
            period_data = xr.concat(model_das, dim='model')
            
            if len(period_data.model) == 0:
                logger.warning(f"No models found for period {start_yr}-{end_yr}")
                continue
            
            logger.info(f"Computing ensemble stats for period {start_yr}-{end_yr} "
                       f"with {len(period_data.model)} models")
            
            # Compute statistics for this period
            stats = compute_ensemble_stats(period_data)
            
            # Ensure start_year and end_year are preserved in the output
            for stat_var in stats.data_vars:
                if 'start_year' not in stats[stat_var].dims:
                    stats[stat_var] = stats[stat_var].expand_dims(start_year=[start_yr])
                if 'end_year' not in stats[stat_var].dims:
                    stats[stat_var] = stats[stat_var].expand_dims(end_year=[end_yr])
            
            period_stats.append(stats)

            # Clear from memory
            del periods_dict[period_key]

        
        if not period_stats:
            logger.error(f"No statistics computed for {var_name}")
            continue
        
        # Combine all periods
        logger.info("Combining statistics across all periods")
        ensemble_stats = xr.concat(period_stats, dim=['start_year', 'end_year'])
        
        # Add global attributes
        
        ensemble_stats.attrs.update({
            'description': f'Ensemble statistics for {var_name}',
            'scenario': scenario,
            'variable': var_name,
            'models_used': model_names,
            'number_of_models': len(model_names),
            'time_periods': [f"{s}-{e}" for s, e in sorted(periods_dict.keys())],
            'methodology': (
                'Precipitation extreme values are calculated over multi-year periods '
                'for statistical robustness. Each period is identified by start_year '
                'and end_year dimensions. Ensemble statistics are computed across all '
                'available models for each period separately.'
            )
        })
        
        # Construct output path
        output_zarr = f"{var_name}_decade_month_{scenario}.zarr"
        output_path = f"s3://{output_bucket}/{output_prefix}/{scenario}/{output_zarr}"
        
        # Save to S3
        logger.info(f"Saving ensemble statistics to {output_path}")
        try:
            fs = s3fs.S3FileSystem(anon=False)
            store = s3fs.S3Map(root=output_path, s3=fs)
            
            # Persist and save
            with dask.diagnostics.ProgressBar():
                ensemble_stats.to_zarr(store, mode='w', consolidated=True)
            
            output_uris[var_name] = output_path
            logger.info(f"Successfully saved {var_name} ensemble")
            
        except Exception as e:
            logger.error(f"Error saving ensemble for {var_name}: {e}")
            raise
    
    return output_uris