# Experiment with GeoJSON Infrastructure + Climate TIFFs

## Overview

This directory combines approaches from `research/frontend/geojson` and `research/tile_service/dynamic_tiler` to generate a map app that shows both infrastructure and climate data visualized on a single map. It uses a dash app and dash-leaflet as the front end.

The sample climate data in the `data/` directory is % burnt area mean over ~50 years. This can be replaced by a custom COG (Cloud Optimized GeoTIFF) of your choosing. See `research/tile_service/dynamic_tiler/data` for notebook on generating GeoTIFFs for climate data.



## How to run
0. Set up a local postgres instance and load using the steps in `backend/physical-asset/database` and `backend/physical-asset/etl`

1. Run the docker compose in this directory to start TiTiler, which serves the climate tiles.

```bash
docker compose up --build
```

2. Create a conda env to install necessary packages and activate
```bash
conda env create -f app/environment.yml
```

```bash
conda activate frontend-geojson-climate
```

3. Create `.env` file based on `env.sample`. The database details will need to be set based on the database you are connecting to.

4. Run the dash app and go to `http://127.0.0.1:8050/` in your broswer to see the map.

```bash
python app/app.py
```

## Limitations

When experimenting, loading the infrastructure points data as a GeoJSON layer in dash-leaflet proved to crash the browser. This is likely due to the browser trying to render >100k points at once. This shows there is an inherent limitation with using GeoJSON data in the mapping application this way. 

The solution would be to use something like Tegola to serve vector tile data from the PostGIS database. Dash-Leaflet does not support vector tiles. An alternative is to enable clustering, which forces the browser not to render all the features at one time. This does not look as visually appealing as a vector tile approach would. 