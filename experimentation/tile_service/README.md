
# Tiling Service

This directory contains a set of experimental scripts, images, and a Dash application to test the use of [TiTiler](https://developmentseed.org/titiler/), an open source dyanmic tile server.

The goal is to visualize gridded climate data and points of interest features on a map application.

## How to Use
### 1. Data Generation

In the `/data` directory, there is a small Jupyter notebook that gives an example of processing a NetCDF file using the Xarray package, and outputting a Cloud Optimized GeoTIFF file (COG). A COG is the optimal data file type to use with TiTiler, and outputs with a .tif extension.

Use the notebook with your own input NetCDF (or any input of your choice), and output the COG from the Xarray dataset as `OutputCOG.tif` in the `/data` directory.

Vector data will come from a PostGIS database.

### 2. Run the Tile Service Stack

First, you will need to have Docker installed on your machine. Please follow [these instructions](https://docs.docker.com/engine/install/) to setup if needed.

Once installed, run the following command in the current directory.

```bash
docker compose up --build
```

This will begin running both the [TiTiler](https://developmentseed.org/titiler/) application (raster tiler) and the custom "file serving" script. The file serving script allows TiTiler to make partial GET requests to our output data file. In production, we would likely store the data on a cloud provider that allows partial GET requests directly to the object. The file serve script is simply for local testing.

This will also run a vector tiler, currently [Tegola](https://tegola.io/), to serve vector tiles of point of interest. This can be configured in `vector_tiler/tegola` to serve tiles from a PostGIS database.

### 3. Run the Dash App

Finally we will run the map application. Navigate to the `climate-risk-map/experimentation/frontend/` directory and run one of the scripts from the various front ends. For example
in the `climate-risk-map/experimentation/frontend/dash_leaflet/geojson/app`, which uses dash-leaflet, a geojson component, and raster tile component to create the mapping application.

```bash
cd /climate-risk-map/experimentation/frontend/dash_leaflet/geojson/app
python app.py
```

The app should now be running on your local machine and can be accessed by going to http://127.0.0.1:8050 in your web browser.

*Note* The dash app was not added to the docker compose stack because of isseus with CORS (Cross-Origin Resource Sharing). When added, the interaction with TiTiler was blocked while running locally. Given this is simply for experimentation, more time was not spent troubleshooting to find a solution. 