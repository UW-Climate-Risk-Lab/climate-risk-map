import pandas as pd
import json
import re
import os

def parse_numeric_with_unit(value):
    """
    Parse string values that contain a numeric value with a unit (e.g. "413 MW").
    Returns a dictionary with 'value' (numeric) and 'unit' (string).
    """
    if pd.isna(value) or value is None or not isinstance(value, str):
        return {'value': None, 'unit': None}
    
    # Use regex to extract numeric part and unit
    import re
    numeric_match = re.search(r'(\d+(?:\.\d+)?)', value)
    unit_match = re.search(r'\d+(?:\.\d+)?\s*([a-zA-Z]+)', value)
    
    numeric_value = float(numeric_match.group(1)) if numeric_match else None
    unit = unit_match.group(1) if unit_match else None
    
    # Convert to integer if the float value has no decimal part
    if numeric_value is not None and numeric_value.is_integer():
        numeric_value = int(numeric_value)
    
    return {'value': numeric_value, 'unit': unit}

def parse_osm_tags(tags_str):
    """
    Parse OpenStreetMap (OSM) tag string into a dictionary.
    
    This function handles multiple possible formats for OSM tags:
    - JSON format: {"key1":"value1","key2":"value2"}
    - Semicolon-separated key-value pairs: key1=value1;key2=value2
    """
    # Initialize empty dictionary for parsed tags
    parsed_tags = {}
    
    # Check if the tags string is empty or NaN
    if pd.isna(tags_str) or tags_str == '':
        return parsed_tags
    
    # Try to parse as JSON
    try:
        parsed_tags = json.loads(tags_str)
        return parsed_tags
    except:
        pass
    
    # Try to parse as semicolon-separated key=value pairs
    try:
        for pair in tags_str.split(';'):
            if '=' in pair:
                key, value = pair.split('=', 1)
                parsed_tags[key.strip()] = value.strip()
        if parsed_tags:  # If we successfully parsed any tags
            return parsed_tags
    except:
        pass
    
    # If all else fails, just return the original string as a single tag
    return {"original_tag": tags_str}

def extract_infrastructure_attributes(df, infrastructure_type):
    """
    Extract specific attributes based on infrastructure type and add as new columns.
    """
    # Apply the parse_tags function to the tags column
    parsed_tags = df['tags'].apply(parse_osm_tags)
    
    # Create a list of all unique keys across all parsed tag dictionaries
    all_keys = set()
    for tags_dict in parsed_tags:
        all_keys.update(tags_dict.keys())
    
    print(f"Found the following tag keys in {infrastructure_type} data:")
    for key in sorted(all_keys):
        print(f"  - {key}")
    
    # Common columns for all infrastructure types
    df['name'] = parsed_tags.apply(lambda x: x.get('name', None))
    
    # Parse capacity with its unit
    capacity_values = parsed_tags.apply(lambda x: x.get('capacity', None))
    df['capacity_raw'] = capacity_values
    parsed_capacity = capacity_values.apply(parse_numeric_with_unit)
    df['capacity'] = parsed_capacity.apply(lambda x: x['value'])
    df['capacity_unit'] = parsed_capacity.apply(lambda x: x['unit'])
    
    df['operator'] = parsed_tags.apply(lambda x: x.get('operator', None))
    
    # Type-specific columns
    if infrastructure_type == 'storage':
        df['storage_type'] = parsed_tags.apply(lambda x: x.get('storage', x.get('storage_type', None)))
        df['material'] = parsed_tags.apply(lambda x: x.get('material', None))
        df['building_levels'] = parsed_tags.apply(lambda x: x.get('building:levels', None))
        
    elif infrastructure_type == 'power':
        df['power_type'] = parsed_tags.apply(lambda x: x.get('power', None))
        
        # Parse voltage with its unit
        voltage_values = parsed_tags.apply(lambda x: x.get('voltage', None))
        df['voltage_raw'] = voltage_values
        parsed_voltage = voltage_values.apply(parse_numeric_with_unit)
        df['voltage'] = parsed_voltage.apply(lambda x: x['value'])
        df['voltage_unit'] = parsed_voltage.apply(lambda x: x['unit'])
        
        df['generator_source'] = parsed_tags.apply(lambda x: x.get('generator:source', None))
        df['generator_type'] = parsed_tags.apply(lambda x: x.get('generator:type', None))
        
        # Parse generator output with its unit
        generator_output_values = parsed_tags.apply(lambda x: x.get('generator:output', None))
        df['generator_output_raw'] = generator_output_values
        parsed_output = generator_output_values.apply(parse_numeric_with_unit)
        df['generator_output'] = parsed_output.apply(lambda x: x['value'])
        df['generator_output_unit'] = parsed_output.apply(lambda x: x['unit'])
        
    elif infrastructure_type == 'datacenter':
        df['building_levels'] = parsed_tags.apply(lambda x: x.get('building:levels', None))
        df['cooling_type'] = parsed_tags.apply(lambda x: x.get('cooling', x.get('cooling_type', None)))
        df['power_usage'] = parsed_tags.apply(lambda x: x.get('power_usage', None))
        df['backup_generator'] = parsed_tags.apply(lambda x: x.get('backup_generator', None))
    
    # Create additional columns for any tag key found in the data
    for key in all_keys:
        if key not in ['name', 'capacity', 'operator'] and not any(col.endswith(key.replace(':', '_')) for col in df.columns):
            # Create a clean column name (remove spaces, special chars)
            col_sub = re.sub(r'[^\w]', '_', key)
            col_name = f"tag_{col_sub}"
            # Extract values for this key from parsed_tags
            df[col_name] = parsed_tags.apply(lambda x: x.get(key, None))
    
    return df

def process_csv(input_file, output_file, infrastructure_type):
    """
    Process a CSV file by parsing the tags column and extracting useful information.
    """
    # Read the CSV file
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    # Check if 'tags' column exists
    if 'tags' not in df.columns:
        print(f"Error: 'tags' column not found in {input_file}")
        return None
    
    # Extract infrastructure-specific attributes
    df = extract_infrastructure_attributes(df, infrastructure_type)
    
    # Save to the output file
    print(f"Saving processed data to {output_file}...")
    df.to_csv(output_file, index=False)
    
    print(f"Successfully processed {input_file}")
    return df

def main():
    """
    Main function to process all three CSV files for the Eastern Washington 
    Infrastructure Resilience Challenge.
    """
    print("Eastern Washington Infrastructure Resilience Challenge - Data Processing")
    print("=====================================================================")
    
    # Define input and output file paths with corresponding infrastructure types
    file_info = [
        {
            'input': "data/storage-rental-exposure-raw.csv",
            'output': "data/storage-rental-exposure-processed.csv",
            'type': 'storage'
        },
        {
            'input': "data/power-plants-exposure-raw.csv",
            'output': "data/power-plants-exposure_processed.csv",
            'type': 'power'
        },
        {
            'input': "data/data-center-exposure-raw.csv",
            'output': "data/data-center-exposure_processed.csv",
            'type': 'datacenter'
        }
    ]
    
    # Process each file
    for info in file_info:
        df = process_csv(info['input'], info['output'], info['type'])
    
    print("\nAll files processed successfully!")
    print("\nNext steps for students:")
    print("1. Review the processed CSV files to understand the infrastructure attributes")
    print("2. Use the Fire Weather Index (FWI) data to assess risk exposure")
    print("3. Develop mitigation strategies based on asset vulnerabilities")
    print("4. Prepare investment pitch for your assigned portfolio")

if __name__ == "__main__":
    main()