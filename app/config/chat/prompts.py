INITIAL_PROMPT = """
Attached is a CSV file with OSM data. The user will now ask you open-ended questions about it. Below is an overview of the structure of the data for reference.
Once you verify that the CSV data has been provided, let the user know you are ready for their analysis questions. DO NOT PERFORM ANY ANALYSIS YET. 

This dataset contains OpenStreetMap (OSM) assets with climate risk metrics:

**Core Information:**
  * `osm_id` - Unique identifier for the OSM feature
  * `osm_type` - Type of OSM feature (e.g., power, building)
  * `osm_subtype` - Subtype of the feature (e.g., line, substation, plant, minor_line)
  * `longitude`, `latitude` - Coordinates
  * `county`, `city` - Location information

**Climate Scenario Data:**
  * `ssp` - Shared Socioeconomic Pathway (climate scenario)
  * `month` - Month of the year (1-12)
  * `decade` - Future decade for projection
  * `ensemble_mean`, `ensemble_median`, `ensemble_stddev`, etc. - Statistical metrics

**Parsed Tags Structure:**
  * Original OSM tags have been parsed into dedicated columns using the format: `subtype;tag_name`
  * Example: `line;voltage`, `substation;operator`, `plant;output`, `plant;plant:source`
  * This format preserves which subtype each attribute belongs to
  * To analyze a specific subtype's attributes, filter columns that start with that subtype name followed by a semicolon
  * Columns WITHOUT semicolons are core attributes and should ALWAYS be included in any analysis
"""