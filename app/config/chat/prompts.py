INITIAL_PROMPT = """
I have uploaded a CSV file containing OpenStreetMap (OSM) infrastructure data with associated climate risk metrics for our analysis session.
Below is an overview of the key data columns you'll be working with:

**Core Asset Information:**
* `osm_id`: Unique OpenStreetMap identifier for the asset.
* `osm_type`: General type of OSM feature (e.g., 'power').
* `osm_subtype`: Specific type of asset (e.g., 'line', 'substation', 'plant', 'minor_line'). This is key for differentiating asset roles.
* `longitude`, `latitude`: Geographic coordinates.
* `county`, `city`: Administrative location information, crucial for regional risk assessment.

**Climate Risk & Scenario Data:**
* `ssp`: The Shared Socioeconomic Pathway (climate scenario) for the projection (e.g., 585 for SSP5-8.5).
* `month`: The month for which the projection is valid (e.g., 8 for August).
* `decade`: The future decade of the projection (e.g., 2030).
* `ensemble_max`: **This is the primary climate risk indicator value (e.g., Fire Weather Index). Use this for your core risk analysis.**

**Specific Asset Attributes (Parsed OSM Tags):**
* Many columns are prefixed with the `osm_subtype` followed by a semicolon and the tag name (e.g., `line;voltage`, `plant;name`, `substation;operator`).
* These columns provide detailed characteristics of the assets. For example, to analyze voltage of power lines, you would look at the `line;voltage` column, filtering for rows where `osm_subtype` is 'line'.
* Columns WITHOUT semicolons are core attributes and generally apply to all assets in the row.

I am now ready for your analysis questions about this dataset. Let me know you're ready. DO NOT PERFORM ANY ANALYSIS YET. Just confirm you have received the data context and are prepared.
"""