from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from mangum import Mangum

from . import api

app = FastAPI(
    title="Climate Risk Data API",
    description="An API for accessing climate risk data.",
    version="0.1.0"
)

app.include_router(api.router)

@app.get("/", response_class=HTMLResponse)
def root():
    return """
    <html>
        <head>
            <title>Climate Risk Data API</title>
            <style>
                :root {
                    --primary-color: #2c3e50;
                    --secondary-color: #3498db;
                    --background-color: #f8f9fa;
                    --code-background: #f2f3f4;
                }
                body { 
                    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
                    max-width: 1000px; 
                    margin: 0 auto; 
                    padding: 2rem;
                    line-height: 1.6;
                    color: var(--primary-color);
                    background-color: var(--background-color);
                }
                .container {
                    background: white;
                    padding: 2rem;
                    border-radius: 10px;
                    box-shadow: 0 2px 15px rgba(0,0,0,0.1);
                }
                pre { 
                    background-color: var(--code-background);
                    padding: 1.5rem;
                    border-radius: 8px;
                    overflow-x: auto;
                    border: 1px solid #e1e4e8;
                }
                code { 
                    background-color: var(--code-background);
                    padding: 0.2em 0.4em;
                    border-radius: 3px;
                    font-size: 0.9em;
                }
                h1, h2 { 
                    color: var(--primary-color);
                    border-bottom: 2px solid var(--secondary-color);
                    padding-bottom: 0.5rem;
                }
                h1 {
                    font-size: 2.5rem;
                    margin-bottom: 2rem;
                }
                .parameter-list {
                    display: grid;
                    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
                    gap: 1rem;
                }
                .parameter-item {
                    background: white;
                    padding: 1rem;
                    border-radius: 6px;
                    border: 1px solid #e1e4e8;
                }
                .warning {
                    background-color: #fff3cd;
                    border: 1px solid #ffeeba;
                    padding: 1rem;
                    border-radius: 6px;
                    margin: 1rem 0;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Climate Risk Data API</h1>
                
                <p>This API provides access to climate risk data across various geographical regions, supporting both GeoJSON and CSV formats for easy integration with data analysis workflows.</p>
                
                <h2>Quick Start</h2>
                <p>To use the API, you'll need:</p>
                <ul>
                    <li>An API key for authentication</li>
                    <li>Python with the <code>requests</code> package</li>
                    <li>For data analysis: <code>pandas</code> and <code>geopandas</code> (optional)</li>
                </ul>

                <h2>Code Example</h2>
                <p>Here's a complete example showing how to fetch and load climate data:</p>
                <pre>
import requests
import pandas as pd
import geopandas as gpd

# API Configuration
url = "https://tf5k4ogi6g.execute-api.us-east-2.amazonaws.com/v1-dev"
headers = {
    "x-api-key": "your_api_key_here"
}
format = "csv" # Can also be 'geojson'

# Example query for Fire Weather Index (FWI) data
query = f"{url}/data/{format}/infrastructure/power/"
params = {
    "osm_subtype": "line",
    "climate_variable": "fwi",
    "climate_ssp": "126",
    "climate_month": "8",
    "climate_decade": "2010"
}

# Make request
response = requests.get(query, params=params, headers=headers).json()
download_url = response["download_url"]

# Load data based on format
if format == "csv":
    # For CSV format (includes WKT geometry)
    df = pd.read_csv(download_url, compression="gzip")
    # Optionally convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df, 
        geometry=gpd.GeoSeries.from_wkt(df['geometry_wkt']),
        crs="EPSG:4326"
    )
else:
    # For GeoJSON format
    gdf = gpd.read_file(download_url)
                </pre>

                <div class="warning">
                    Note: The download URL is temporary and will expire after 1 hour.
                </div>

                <h2>Available Parameters</h2>
                <div class="parameter-list">
                    <div class="parameter-item">
                        <h3>Core Parameters</h3>
                        <ul>
                            <li><code>format</code>: 'csv' or 'geojson'</li>
                            <li><code>osm_category</code>: Category of features</li>
                            <li><code>osm_type</code>: Type of features</li>
                        </ul>
                    </div>
                    
                    <div class="parameter-item">
                        <h3>Climate Parameters</h3>
                        <ul>
                            <li><code>climate_variable</code>: e.g., 'fwi'</li>
                            <li><code>climate_ssp</code>: SSP number</li>
                            <li><code>climate_month</code>: 1-12</li>
                            <li><code>climate_decade</code>: e.g., 2010</li>
                        </ul>
                    </div>

                    <div class="parameter-item">
                        <h3>Optional Parameters</h3>
                        <ul>
                            <li><code>bbox</code>: Geographic bounds</li>
                            <li><code>epsg_code</code>: Coordinate system</li>
                            <li><code>geom_type</code>: Geometry filter</li>
                            <li><code>limit</code>: Result limit</li>
                        </ul>
                    </div>
                </div>

                <h2>OpenStreetMap Data Structure</h2>
                <style>
                    .osm-grid {
                        display: grid;
                        grid-template-columns: repeat(auto-fill, minmax(450px, 1fr));
                        gap: 2rem;
                        margin: 2rem 0;
                    }
                    .osm-category {
                        background: white;
                        border-radius: 8px;
                        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                        padding: 1.5rem;
                    }
                    .osm-category h3 {
                        color: var(--secondary-color);
                        margin-top: 0;
                        display: flex;
                        justify-content: space-between;
                        align-items: center;
                    }
                    .osm-type {
                        margin: 1rem 0;
                        padding: 1rem;
                        background: var(--background-color);
                        border-radius: 4px;
                        border-left: 4px solid var(--secondary-color);
                    }
                    .osm-subtype-list {
                        margin: 0.8rem 0 0 1.2rem;
                        padding-left: 1rem;
                        border-left: 2px solid #e1e4e8;
                    }
                    .wiki-link {
                        color: var(--secondary-color);
                        text-decoration: none;
                        font-size: 0.8em;
                        padding: 2px 6px;
                        border-radius: 4px;
                        background: rgba(52, 152, 219, 0.1);
                    }
                    .wiki-link:hover {
                        background: rgba(52, 152, 219, 0.2);
                    }
                    .osm-description {
                        font-size: 0.9em;
                        color: #666;
                        margin-bottom: 1rem;
                        padding: 0.5rem;
                        background: #f8f9fa;
                        border-radius: 4px;
                    }
                    .osm-label {
                        font-size: 0.7em;
                        text-transform: uppercase;
                        background: #e1e4e8;
                        padding: 2px 6px;
                        border-radius: 3px;
                        margin-right: 0.5rem;
                    }
                </style>
                
                <div class="osm-grid">
                    <!-- Infrastructure Category -->
                    <div class="osm-category">
                        <h3>
                            <span><span class="osm-label">Category</span> Infrastructure</span>
                        </h3>
                        <p class="osm-description">Physical infrastructure and utilities</p>
                        
                        <div class="osm-type">
                            <strong><span class="osm-label">Type</span> Power</strong>
                            <a href="https://wiki.openstreetmap.org/wiki/Key:power" class="wiki-link">[wiki]</a>
                            <div class="osm-subtype-list">
                                <span class="osm-label">Subtypes</span>
                                <br>
                                • Tower <a href="https://wiki.openstreetmap.org/wiki/Tag%3Apower%3Dtower" class="wiki-link">[wiki]</a><br>
                                • Line <a href="https://wiki.openstreetmap.org/wiki/Tag:power%3Dline" class="wiki-link">[wiki]</a><br>
                                • Plant <a href="https://wiki.openstreetmap.org/wiki/Tag:power%3Dplant" class="wiki-link">[wiki]</a><br>
                                • Substation <a href="https://wiki.openstreetmap.org/wiki/Tag:power%3Dsubstation" class="wiki-link">[wiki]</a>
                            </div>
                        </div>
                    </div>

                    <!-- Amenity Category -->
                    <div class="osm-category">
                        <h3>
                            <span><span class="osm-label">Category</span> Amenity</span>
                        </h3>
                        <p class="osm-description">Community facilities and services</p>
                        
                        <div class="osm-type">
                            <strong><span class="osm-label">Type</span> Hospital</strong>
                            <a href="https://wiki.openstreetmap.org/wiki/Key:hospital class="wiki-link">[wiki]</a>
                            <p class="osm-description">No subtypes available for this type</p>
                        </div>
                    </div>

                    <!-- Place Category -->
                    <div class="osm-category">
                        <h3>
                            <span><span class="osm-label">Category</span> Place</span>
                        </h3>
                        <p class="osm-description">Administrative and named areas</p>
                        
                        <div class="osm-type">
                            <strong><span class="osm-label">Type</span> Boundary</strong>
                            <a href="https://wiki.openstreetmap.org/wiki/Key:boundary" class="wiki-link">[wiki]</a>
                            <p class="osm-description">No subtypes available for this type</p>
                        </div>
                    </div>
                </div>

                <div class="warning">
                    Note: This is not an exhaustive list. Only commonly used types and subtypes are shown.
                    Visit the OpenStreetMap wiki for complete documentation.
                </div>

                <h2>Need Help?</h2>
                <p>For detailed API documentation, visit:</p>
                <ul>
                    <li>Interactive API docs: <code>/docs</code></li>
                    <li>ReDoc documentation: <code>/redoc</code></li>
                </ul>
            </div>
        </body>
    </html>
    """

# AWS Lambda handler
handler = Mangum(app)