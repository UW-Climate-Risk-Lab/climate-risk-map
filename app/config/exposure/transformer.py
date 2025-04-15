import logging

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
    properties = feature.get("properties", {})
    # Handle both direct properties and tags
    for prop in properties:
        if prop in ["voltage", "VOLTAGE"] and isinstance(properties[prop], str):
            voltage = properties[prop]
            try:
                voltage = float(voltage)
                voltage = voltage / 1000.
            except ValueError as e:
                if ";" in voltage:
                    voltage_list = str(voltage).split(";")
                    voltage_list_numeric = list()
                    # Found edge case where the voltage value was 230000;?
                    # want to only take numeric values
                    for v in voltage_list:
                        try:
                            voltage_list_numeric.append(float(v))
                        except Exception as e:
                            continue
                    if len(voltage_list_numeric) > 0:
                        voltage = max(voltage_list_numeric) / 1000.
                    else:
                        pass
                else:
                    logger.error(f"Unable to parse OSM voltage '{str(voltage)}': {str(e)}")
            except Exception as e:
                logger.error(f"Unable to parse OSM voltage: {str(e)}")
            properties[prop] = voltage

    return feature