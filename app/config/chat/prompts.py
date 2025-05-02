INITIAL_PROMPT = """
Analyze the provided single CSV dataset containing OSM asset data and projected Canadian FWI wildfire risk.

**Data Overview:**
* **Assets:** Could be power, commercial, agriculture, data centers, etc. Determine specific types from `osm_type`, `osm_subtype`, and especially the `tags` column.
* **Risk Metric:** Focus on `ensemble_median` FWI. Note the context (`ssp`, `month`, `decade`).
* **Location:** Use `latitude`, `longitude`, `county`, `city`.
* **Critical Details (`tags` column):** Contains JSON strings detailing each asset. **Parsing this column is ESSENTIAL** for understanding asset specifics and assessing criticality. Prepare for potential errors during JSON parsing (e.g., handle invalid entries gracefully in your code).

**Required Analysis Steps:**
Using your Python environment and standard framework:
1.  **Load & Prepare Data:** Load the CSV. Parse the `tags` column JSON (handle errors). Ensure FWI and location columns are numeric.
2.  **Identify Assets:** Summarize the specific asset types found in *this file*, using details from the parsed `tags`.
3.  **Analyze FWI Risk:** Analyze the `ensemble_median` FWI distribution impacting these assets.
4.  **Identify High-Risk Critical Assets:** List critical assets (identified via `tags`) with the highest FWI exposure.
5.  **Discuss Implications:** Briefly explain the findings and potential consequences.

**Output Format:**
* Respond using MARKDOWN.
* Use `##` or lower headings (NO `#` level headings).
* Be concise but thorough.
"""