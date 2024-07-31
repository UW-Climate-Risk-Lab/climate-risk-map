# Experiment with GeoJSON Infrastructure + Climate TIFFs

## Overview

This directory uses a dash app that shows both infrastructure and climate data visualized on a single map. It uses the dash-leaflet library.

The sample climate data in the `experimentation/tile_service/raster_tiler/dynamic/data` directory is % burnt area mean over ~50 years. This can be replaced by a custom COG (Cloud Optimized GeoTIFF) of your choosing. See `experimentation/tile_service/raster_tiler/dynamic/data` for notebook on generating GeoTIFFs for climate data.



## How to run
0. Set up a local postgres instance and load using the steps in `backend/physical-asset/database` and `backend/physical-asset/etl`

1. Navigate to and run the docker compose in `climate-risk-map/experimentation/tile_service` to start the tiling service, which serves the climate tiles.

```bash
docker compose up --build
```

2. Navigate to `climate-risk-map/experimentation/frontend/dash_leaflet/geojson`, create a conda env, and install necessary packages and activate

```bash
conda env create -f app/environment.yml
```

```bash
conda activate frontend-geojson-climate
```

3. Create `.env` file based on `env.sample`. The database details will need to be set based on the database you are connecting to.

4. Run the dash app and go to `http://127.0.0.1:8050/` in your broswer to see the map.

```bash
python app.py
```

## Limitations

When experimenting, loading the infrastructure points data as a GeoJSON layer in dash-leaflet proved to crash the browser. This is likely due to the browser trying to render >100k points at once. This shows there is an inherent limitation with using GeoJSON data in the mapping application this way. 

The solution would be to use something like Tegola to serve vector tile data from the PostGIS database. Dash-Leaflet does not support vector tiles. An alternative is to enable clustering, which forces the browser not to render all the features at one time. This does not look as visually appealing as a vector tile approach would. 