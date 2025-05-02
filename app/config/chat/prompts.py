INITIAL_PROMPT = """
You are receiving a single CSV dataset for analysis.

This file contains OpenStreetMap (OSM) asset data combined with projected Canadian Fire Weather Index (FWI) wildfire risk statistics for a specific region, climate scenario (`ssp`), month, and decade.

The assets described in this file could fall into various categories, such as:
* Power grid infrastructure (lines, substations, etc.)
* Commercial buildings (offices, hotels, general retail)
* Agricultural land (farmland, vineyards)
* Data centers
* Other types identifiable via OSM tags.

You will need to examine the data (using columns like `osm_type`, `osm_subtype`, and especially the parsed `tags` column) to determine the specific types of assets present in *this* particular file.

**Key Data Points in this File for Your Analysis:**

* **Asset Specifics (`tags` column):** This column contains critical, detailed attributes for each asset, formatted as a JSON string. **You MUST parse this column** (e.g., using Python's `json` library within pandas) to understand the specific nature, name, or function of assets, which is crucial for assessing their criticality.
* **Location Data:** Utilize `latitude`, `longitude`, `county`, and `city` columns for geographic context and distribution analysis.
* **Wildfire Risk Metric (`ensemble_median`):** Focus your primary risk analysis on the **`ensemble_median`** FWI column. Use the corresponding `ssp`, `month`, and `decade` values to provide context for the projections. You may reference other `ensemble_*` columns (like `ensemble_max`) for additional context if needed.
* **Asset Identifiers:** Use `osm_id`, `osm_type`, and `osm_subtype` for basic classification.

**Your Task (using your standard framework):**

Apply your established climate risk analysis framework to this specific dataset. For this file, prioritize the following:
1.  Identify and summarize the specific types of assets contained within *this file*, using details extracted from the parsed `tags` column.
2.  Analyze the distribution and levels of projected wildfire risk (`ensemble_median` FWI) impacting these specific assets.
3.  Identify which assets within *this file*, particularly any deemed critical based on their type/function (informed by `tags`), face the highest FWI exposure.
4.  Discuss the potential implications of this risk exposure for the identified assets and the area.

Remember to use your Python environment to load, prepare (including the essential parsing of the `tags` JSON and ensuring numeric FWI values), and analyze this file accurately. Proceed with your educational and insightful analysis.

YOU MUST RESPOND USING MARKDOWN FORMAT, BUT DO NOT USE THE HIGHEST HEADING LEVEL (#).
"""