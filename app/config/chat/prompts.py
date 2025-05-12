INITIAL_PROMPT = """
I have uploaded a CSV file containing OpenStreetMap (OSM) infrastructure data with associated climate and wildfire risk metrics for our analysis session.

Here is an overview of the key data columns you'll be working with:

**Core Asset Information:**
* `osm_id`: Unique OpenStreetMap identifier for the asset.
* `osm_type`: General type of OSM feature (e.g., 'power').
* `osm_subtype`: Specific type of asset (e.g., 'line', 'substation', 'plant'). This is key for differentiating asset roles.
* `longitude`, `latitude`: Geographic coordinates.
* `county`, `city`: Administrative location information, crucial for regional risk assessment.

**Key Risk Metrics:**
* `ensemble_mean`: **Projected Fire Weather Index (FWI)**. This indicates the potential intensity of a fire based *purely on weather conditions* if an ignition occurs. Higher values mean more intense fire weather.
* `month`: The month column refers to the month of the `ensemble_mean` column
* `decade`: The decade column refers to the decade of the `ensemble_mean` column
* `ensemble_mean_historic_baseline`: **Historical Baseline FWI (approx. 1950-2010 mean)**. This provides context for the projected FWI. The *difference* between `ensemble_mean` and this baseline shows the *change* in fire weather intensity potential.
* `burn_probability`: **Annual Burn Probability (USDA, circa 2021)**. This estimates the annual *likelihood* (0.0 to 1.0) of a given 500m location burning, based on landscape factors, fuels (~2021), and simulated fire spread. It complements FWI by indicating how likely a fire is in that specific spot.

**Specific Asset Attributes (Parsed OSM Tags):**
* Many columns are prefixed with the `osm_subtype` followed by a semicolon and the tag name (e.g., `line;voltage`, `plant;name`, `substation;operator`).
* These provide detailed characteristics (like voltage, operator, power source) valuable for targeted analysis.
* Columns WITHOUT semicolons are core attributes and generally apply to all assets in the row.

I am now ready for your analysis questions about this dataset. Let me know you're ready. DO NOT PERFORM ANY ANALYSIS YET. Just confirm you have received the data context and are prepared.
"""