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
        "module": "src.indicators.fwi",
        "function": "calculate_fwi",
        "output_vars": ["dc", "dmc", "ffmc", "fwi"],
    },
    "pr_percent_change": {
        "module": "src.indicators.precip",
        "function": "calculate_precip_percent_change",
        "output_vars": ["pr_percent_change"],
    },
    # SPEI used too much memory for calculation, need to optimze later
    # "spei": {
    #     "module": "indicators.spei",
    #     "function": "calculate_spei",
    #     "spei_scale": 12,
    #     "output_vars": ["spei_12month"],
    # }
}

# Default chunk sizes (can be overridden by args)
DEFAULT_LAT_CHUNK = 150
DEFAULT_LON_CHUNK = 360
DEFAULT_THREADS = 2
DEFAULT_MEMORY = "125" # GB