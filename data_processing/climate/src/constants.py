# src/constants.py
import os

# Existing constants...
INPUT_BUCKET = "nex-gddp-cmip6"
INPUT_PREFIX = "NEX-GDDP-CMIP6"
OUTPUT_BUCKET = os.environ["OUTPUT_S3_BUCKET"]
# Adjusted output prefix if desired, maybe remove /fwi specific part if exists
OUTPUT_PREFIX = "climate-risk-map/backend/climate" 

TIME_CHUNK = -1 # Keep as -1 for full year processing unless memory dictates otherwise

# Define all variables needed by *any* registered indicator
# Add 'tas' if needed for precip change calculations, etc.
VAR_LIST = [
    "tasmax",  # Max Temp (K) - Needed for FWI
    "tasmin",  # Min Temp (K) - Potentially needed for other indicators
    "hurs",    # Relative Humidity (%) - Needed for FWI
    "sfcWind", # Surface Wind Speed (m/s) - Needed for FWI
    "pr",      # Precipitation Flux (kg m-2 s-1) - Needed for FWI, Precip Change
]

VALID_YEARS = {
    "historical": list(range(1950, 2015)),
    "ssp": list(range(2015, 2101))
}

# Define historical range for baseline calculations (e.g., precip change)
HISTORICAL_BASELINE_YEARS = list(range(1981, 2014)) # Example: 30-year baseline

# Indicator Registry
# Maps indicator names to their calculation module and function,
# and lists the output variables they produce.
INDICATOR_REGISTRY = {
    "fwi": {
        "module": "indicators.fwi",
        "function": "calculate_fwi",
        "output_vars": ["dc", "dmc", "ffmc", "fwi"],
        "requires_initial_conditions": True # Specific flag for FWI
    },
    "precip_percent_change": {
        "module": "indicators.precip",
        "function": "calculate_precip_percent_change",
        "output_vars": ["pr_change_percent"],
        "requires_initial_conditions": False,
        # Add flags if it requires historical baseline data
        # "requires_historical_baseline": True 
    },
    # Add more indicators here
    # "example_indicator": {
    #     "module": "src.indicators.example",
    #     "function": "calculate_example",
    #     "output_vars": ["example_var1", "example_var2"],
    #     "requires_initial_conditions": False 
    # }
}

# Default chunk sizes (can be overridden by args)
DEFAULT_LAT_CHUNK = 30
DEFAULT_LON_CHUNK = 72
DEFAULT_THREADS = 2
DEFAULT_MEMORY = "250" # GB