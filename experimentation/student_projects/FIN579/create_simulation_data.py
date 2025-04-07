import pandas as pd
import random
import numpy as np
import re # For cleaning plant_source

# --- Configuration ---

# Asset Managers and their territories/specializations
ASSET_MANAGERS = {
    "QDH": {"counties": ["Grant County"], "asset_types": ["data_center"]},
    "INSP": {"counties": ["Spokane County"], "asset_types": ["storage_rental"]},
    "CBPI": {"counties": ["Benton County", "Franklin County", "Chelan County"], "asset_types": ["power_plant"]},
    "YVIG": {"counties": ["Yakima County", "Kittitas County"], "asset_types": ["data_center", "storage_rental", "power_plant"]},
}

# --- Input File Paths ---
DATA_CENTER_IN_FILE = 'data/data-center-exposure_processed.csv'
POWER_PLANT_IN_FILE = 'data/power-plants-exposure_processed.csv'
STORAGE_IN_FILE = 'data/storage-rental-exposure-processed.csv'

# --- Output File Paths ---
# Asset Manager Files (Full Data)
DATA_CENTER_OUT_AM_FILE = 'data/data-center-exposure_ASSET_MANAGER.csv'
POWER_PLANT_OUT_AM_FILE = 'data/power-plants-exposure_ASSET_MANAGER.csv'
STORAGE_OUT_AM_FILE = 'data/storage-rental-exposure_ASSET_MANAGER.csv'

# Investor Files (Obscured Data)
DATA_CENTER_OUT_INV_FILE = 'data/data-center-exposure_INVESTOR.csv'
POWER_PLANT_OUT_INV_FILE = 'data/power-plants-exposure_INVESTOR.csv'
STORAGE_OUT_INV_FILE = 'data/storage-rental-exposure_INVESTOR.csv'

# --- Investor Data Obscuration Settings ---
OBSCURE_PROBABILITY = 0.60 # Probability that metrics for a given asset are obscured for the investor (e.g., 0.3 = 30%)

# List of SIMULATED columns to potentially obscure for the investor version
# Keep core identifiers, FWI, location, Asset_Manager_Simulated, Asset_Value_M_USD_Simulated
COLS_TO_OBSCURE = [
    'Annual_Revenue_M_USD_Simulated',
    'Facility_Age_Simulated',
    # Data Center Specific
    'Power_Source_Redundancy_Simulated',
    'Customer_Concentration_Simulated',
    # Power Plant Specific
    'Transmission_Exposure_Risk_Simulated',
    # Storage Specific
    'Construction_Type_Simulated',
    'Customer_Type_Simulated',
    # Add others here if desired (but avoid base identifiers, FWI, location, etc.)
]


# --- Helper Functions (Mostly unchanged, generate_financials slightly adjusted) ---

def assign_asset_manager(county, asset_type):
    """Assigns an asset manager based on county and asset type."""
    assigned_manager = "Unaffiliated_Simulated"
    for manager, details in ASSET_MANAGERS.items():
        if county in details["counties"] and asset_type in details["asset_types"]:
            assigned_manager = manager
            break
    return assigned_manager

def generate_static_properties(asset_type, subtype=None):
    """Generates plausible fictitious STATIC properties like value, revenue, age, etc."""
    asset_value_m = 0.0
    annual_revenue_m = 0.0
    facility_age = 0
    generated_props = {} # Dictionary to hold results

    # Base values and age based on asset type and simulated subtype
    if asset_type == 'data_center':
        if subtype == 'Hyperscale':
            asset_value_m = random.uniform(200, 1200)
            facility_age = random.randint(3, 15)
        elif subtype == 'Edge':
            asset_value_m = random.uniform(20, 150)
            facility_age = random.randint(2, 10)
        elif subtype == 'Regional':
            asset_value_m = random.uniform(50, 300)
            facility_age = random.randint(5, 25)
        else: # Colocation or Unknown
            asset_value_m = random.uniform(30, 400)
            facility_age = random.randint(5, 30)
        annual_revenue_m = asset_value_m * random.uniform(0.10, 0.30) # Plausible revenue %
        power_redundancy = random.choices(['Full', 'Partial N+1', 'Partial N', 'Basic UPS Only'], weights=[0.4, 0.3, 0.2, 0.1], k=1)[0]
        customer_concentration = random.choices(['High (Few Tenants)', 'Medium', 'Low (Many Tenants)'], weights=[0.3, 0.5, 0.2], k=1)[0]

        generated_props = {
            'Asset_Value_M_USD_Simulated': round(asset_value_m, 2),
            'Annual_Revenue_M_USD_Simulated': round(annual_revenue_m, 2),
            'Facility_Age_Simulated': facility_age,
            'Power_Source_Redundancy_Simulated': power_redundancy,
            'Customer_Concentration_Simulated': customer_concentration
        }

    elif asset_type == 'power_plant':
        facility_age = random.randint(10, 60)
        if subtype == 'nuclear':
            asset_value_m = random.uniform(1000, 5000)
        elif subtype == 'hydro':
            asset_value_m = random.uniform(200, 2500)
        elif subtype == 'wind':
            asset_value_m = random.uniform(50, 500)
        elif subtype == 'solar':
            asset_value_m = random.uniform(20, 300)
        elif subtype == 'gas':
             asset_value_m = random.uniform(150, 800)
        elif subtype == 'biomass':
             asset_value_m = random.uniform(30, 150)
        else: # Other (coal, oil, unknown)
            asset_value_m = random.uniform(100, 1000)
        annual_revenue_m = asset_value_m * random.uniform(0.05, 0.20) # Plausible revenue %
        transmission_risk = random.choices(['High (Remote/Single Line)', 'Medium', 'Low (Networked/Near Substation)'], weights=[0.35, 0.45, 0.20], k=1)[0]

        generated_props = {
            'Asset_Value_M_USD_Simulated': round(asset_value_m, 2),
            'Annual_Revenue_M_USD_Simulated': round(annual_revenue_m, 2),
            'Facility_Age_Simulated': facility_age,
            'Transmission_Exposure_Risk_Simulated': transmission_risk
        }

    elif asset_type == 'storage_rental':
        if subtype == 'Climate-Controlled Specialty':
            asset_value_m = random.uniform(10, 60)
        elif subtype == 'Business/Data Storage':
            asset_value_m = random.uniform(8, 40)
        elif subtype == 'Agricultural Storage':
             asset_value_m = random.uniform(3, 30)
        else: # Traditional Self-Storage
            asset_value_m = random.uniform(5, 50)
        facility_age = random.randint(5, 40)
        annual_revenue_m = asset_value_m * random.uniform(0.08, 0.22) # Plausible revenue %
        construction = random.choices(['Lightweight Metal', 'Concrete Tilt-Up', 'Wood Frame', 'Mixed'], weights=[0.5, 0.2, 0.15, 0.15], k=1)[0]
        customer = random.choices(['Primarily Residential', 'Primarily Commercial/Business', 'Mixed Use'], weights=[0.6, 0.2, 0.2], k=1)[0]

        generated_props = {
            'Asset_Value_M_USD_Simulated': round(asset_value_m, 2),
            'Annual_Revenue_M_USD_Simulated': round(annual_revenue_m, 2),
            'Facility_Age_Simulated': facility_age,
            'Construction_Type_Simulated': construction,
            'Customer_Type_Simulated': customer
        }
    else: # Default/Fallback
       generated_props = {
            'Asset_Value_M_USD_Simulated': round(random.uniform(5, 100), 2),
            'Annual_Revenue_M_USD_Simulated': round(random.uniform(1, 20), 2),
            'Facility_Age_Simulated': random.randint(5, 40),
       }
    return generated_props

def assign_dc_subtype(manager):
    """Assigns a plausible data center subtype"""
    if manager == 'QDH':
        return random.choices(['Hyperscale', 'Edge'], weights=[0.70, 0.30], k=1)[0]
    elif manager == 'YVIG':
         return random.choices(['Regional', 'Colocation', 'Edge'], weights=[0.5, 0.3, 0.2], k=1)[0]
    else: # Unaffiliated or other
        return random.choices(['Colocation', 'Regional', 'Edge'], weights=[0.5, 0.3, 0.2], k=1)[0]

def assign_storage_subtype(manager):
    """Assigns a plausible storage subtype"""
    if manager == 'INSP':
        return random.choices(
            ['Traditional Self-Storage', 'Climate-Controlled Specialty', 'Business/Data Storage'],
            weights=[0.60, 0.30, 0.10], k=1)[0]
    elif manager == 'YVIG':
        return random.choices(
            ['Agricultural Storage', 'Traditional Self-Storage', 'Climate-Controlled Specialty'],
             weights=[0.40, 0.40, 0.20], k=1)[0]
    else: # Unaffiliated or other
       return random.choice(['Traditional Self-Storage', 'Climate-Controlled Specialty', 'Vehicle/RV Storage'])

def clean_plant_source(source_str):
    """Cleans the plant_source string to get a usable subtype."""
    if pd.isna(source_str):
        return 'unknown'
    source_str = str(source_str).lower()
    # Prioritize specific types
    if 'nuclear' in source_str or 'fission' in source_str: return 'nuclear'
    if 'hydro' in source_str: return 'hydro'
    if 'solar' in source_str or 'photovoltaic' in source_str: return 'solar'
    if 'wind' in source_str: return 'wind'
    if 'gas' in source_str: return 'gas'
    if 'biomass' in source_str or 'wood' in source_str: return 'biomass'
    if 'coal' in source_str: return 'coal'
    if 'oil' in source_str: return 'oil'
    # General terms
    if 'combustion' in source_str: return 'combustion_engine' # Distinguish if possible
    if 'storage' in source_str and 'battery' in source_str : return 'battery_storage' # Might be useful later

    # Simple cleanup for combined terms if needed
    clean_str = re.sub(r'[^a-z;,/]', '', source_str.split(';')[0].split('/')[0].strip()) # take first if multiple, cleaner separators
    return clean_str if clean_str else 'unknown'


# --- Processing Function ---

def process_and_split_file(input_file, out_am_file, out_inv_file, asset_type_name, subtype_col_name=None, source_col_name=None):
    """Reads, processes, adds labels, and splits data for Asset Managers and Investors."""
    print(f"Processing {asset_type_name} from {input_file}...")
    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file}. Skipping.")
        return
    except Exception as e:
         print(f"Error reading {input_file}: {e}. Skipping.")
         return


    # Identify unique assets and their primary county/source
    unique_assets = df.groupby('osm_id', dropna=False).agg( # keep NaN osm_id if they exist temporarily
        county=('county', 'first'),
        osm_subtype=('osm_subtype', 'first'),
        # Include plant_source only if it's relevant and exists
        **{source_col_name: (source_col_name, 'first')} if source_col_name and source_col_name in df.columns else {}
    ).reset_index()

    # Handle potential NaN osm_ids if necessary (though ideally cleaned earlier)
    unique_assets = unique_assets.dropna(subset=['osm_id'])

    # --- Generate Static Data for each Unique Asset ---
    static_data = {}
    for _, row in unique_assets.iterrows():
        asset_id = row['osm_id']
        county = row['county']
        if pd.isna(county): county = "Unknown County" # Handle missing county


        # 1. Assign Manager
        manager = assign_asset_manager(county, asset_type_name)

        # 2. Determine Subtype
        subtype = 'Unknown' # Default
        if asset_type_name == 'data_center':
            subtype = assign_dc_subtype(manager)
        elif asset_type_name == 'storage_rental':
             subtype = assign_storage_subtype(manager)
        elif asset_type_name == 'power_plant':
            plant_source = row.get(source_col_name, None) # Use .get safely
            subtype = clean_plant_source(plant_source)

        # 3. Generate Static Properties
        properties = generate_static_properties(asset_type_name, subtype=subtype)

        # Store all static data keyed by asset_id
        static_data_entry = {
            'Asset_Manager_Simulated': manager,
            **properties # Add generated financials, age, etc.
        }
         # Add specific subtype column only if relevant
        if subtype_col_name and asset_type_name != 'power_plant':
             static_data_entry[f'{subtype_col_name}_Simulated'] = subtype
        elif asset_type_name == 'power_plant':
              # For power plants, store the derived type from source
              static_data_entry['Plant_Type_Simulated'] = subtype


        static_data[asset_id] = static_data_entry

    # --- Map Static Data back to the main DataFrame ---
    map_df = pd.DataFrame.from_dict(static_data, orient='index')
    map_df.index.name = 'osm_id'
    df_merged_am = pd.merge(df, map_df, on='osm_id', how='left')

    # Ensure generated columns exist even if mapping fails for some rows
    for col in map_df.columns:
        if col not in df_merged_am.columns:
            df_merged_am[col] = pd.NA

    # --- Create Investor Version with Obscured Data ---
    df_merged_inv = df_merged_am.copy()
    unique_ids_in_data = df_merged_inv['osm_id'].unique()
    ids_to_obscure = random.sample(list(unique_ids_in_data), k=int(len(unique_ids_in_data) * OBSCURE_PROBABILITY))

    print(f"Obscuring data for {len(ids_to_obscure)} out of {len(unique_ids_in_data)} unique {asset_type_name} assets for investors.")

    # Get the actual list of columns present in this specific DataFrame to obscure
    cols_in_df_to_obscure = [col for col in COLS_TO_OBSCURE if col in df_merged_inv.columns]

    if cols_in_df_to_obscure:
        # Set values to NaN for selected assets and columns
        obscure_mask = df_merged_inv['osm_id'].isin(ids_to_obscure)
        df_merged_inv.loc[obscure_mask, cols_in_df_to_obscure] = np.nan


    # --- Save Both Versions ---
    try:
        df_merged_am.to_csv(out_am_file, index=False)
        print(f"-> Saved Asset Manager data to {out_am_file}")
        df_merged_inv.to_csv(out_inv_file, index=False)
        print(f"-> Saved Investor data to    {out_inv_file}")
    except Exception as e:
        print(f"Error saving output files for {asset_type_name}: {e}")


# --- Run Processing ---

process_and_split_file(DATA_CENTER_IN_FILE, DATA_CENTER_OUT_AM_FILE, DATA_CENTER_OUT_INV_FILE,
                       'data_center', subtype_col_name='DataCenter_Type')

process_and_split_file(POWER_PLANT_IN_FILE, POWER_PLANT_OUT_AM_FILE, POWER_PLANT_OUT_INV_FILE,
                       'power_plant', source_col_name='plant_source') # Note: uses source_col_name, outputs 'Plant_Type_Simulated'

process_and_split_file(STORAGE_IN_FILE, STORAGE_OUT_AM_FILE, STORAGE_OUT_INV_FILE,
                       'storage_rental', subtype_col_name='Storage_Subtype')

print("\nProcessing complete. Asset Manager and Investor CSV files created.")