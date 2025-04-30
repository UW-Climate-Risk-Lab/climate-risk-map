import logging
import re

logger = logging.getLogger(__name__)

class DataTransformer:
    _transformers = {}
    
    @classmethod
    def register(cls, name):
        def decorator(transform_func):
            cls._transformers[name] = transform_func
            return transform_func
        return decorator
    
    @classmethod
    def apply(cls, data, transformations):
        if transformations:
            for transform_name in transformations:
                if transform_name in cls._transformers:
                    data = cls._transformers[transform_name](data)
        return data


@DataTransformer.register("osm_line_voltage")
def osm_line_voltage(feature):
    """Convert voltage to kV

    Some values of voltage from OpenStreetMap contain multiple values 
    in semi-colon delimited list (example: '115000;200000;200000').
    This represents multiple circuits. If this occurs, we will take the
    max value for display purposes. 

    https://wiki.openstreetmap.org/wiki/Tag:power%3Dline
    
    
    """
    # Mapping for text-based voltage categories based on OSM conventions
    # Values are approximate in kV
    voltage_mapping = {
        "low": 1.0,       # Low voltage: <1000V
        "medium": 20.0,   # Medium voltage: 1kV-50kV
        "high": 110.0,    # High voltage: 50kV-230kV
        "unknown": None,  # Unknown voltage
        "?": None         # Unknown voltage
    }
    
    properties = feature.get("properties", {})
    # Handle both direct properties and tags
    for prop in properties:
        if prop in ["voltage", "VOLTAGE"] and isinstance(properties[prop], str):
            voltage = properties[prop]
            original_voltage = voltage
            
            # Clean up voltage string
            # Remove non-numeric characters except for delimiters ;:/,
            voltage = voltage.lower().strip()
            
            # Check if the voltage is a text category
            if voltage in voltage_mapping:
                properties[prop] = voltage_mapping[voltage]
                continue
                
            try:
                # Try direct conversion first
                voltage_value = float(voltage)
                properties[prop] = voltage_value / 1000.
            except ValueError:
                # Handle various delimiter formats
                voltage_list = []
                
                # Handle semicolons
                if ";" in voltage:
                    delimiter = ";"
                # Handle slashes
                elif "/" in voltage:
                    delimiter = "/"
                # Handle colons
                elif ":" in voltage:
                    delimiter = ":"
                # Handle commas
                elif "," in voltage:
                    delimiter = ","
                else:
                    # Try to extract numeric part using regex
                    # (e.g. for cases like "120kv")
                    match = re.search(r'(\d+)', voltage)
                    if match:
                        try:
                            voltage_value = float(match.group(1))
                            # If the original had "kv" in it, it's already in kV
                            if "kv" in voltage:
                                properties[prop] = voltage_value
                            else:
                                properties[prop] = voltage_value / 1000.
                        except Exception:
                            logger.info(f"Using default for OSM voltage '{original_voltage}'")
                            properties[prop] = None
                    else:
                        logger.info(f"Using default for OSM voltage '{original_voltage}'")
                        properties[prop] = None
                    continue
                
                # Process delimited values
                voltage_parts = voltage.split(delimiter)
                for part in voltage_parts:
                    part = part.strip()
                    if part:
                        try:
                            # Extract numeric part if there are any alphabetic characters
                            match = re.search(r'(\d+)', part)
                            if match:
                                voltage_list.append(float(match.group(1)))
                            else:
                                voltage_list.append(float(part))
                        except Exception:
                            # Skip any parts that can't be converted
                            continue
                
                if voltage_list:
                    max_voltage = max(voltage_list)
                    properties[prop] = max_voltage / 1000.
                else:
                    logger.info(f"Using default for OSM voltage '{original_voltage}'")
                    properties[prop] = None

    return feature