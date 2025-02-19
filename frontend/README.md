# Dash Leaflet Frontend Map

## Overview

This directory uses a dash app that shows both infrastructure and climate data visualized on a single map. It uses the dash-leaflet library.

The sample climate data in the `experimentation/tile_service/raster_tiler/dynamic/data` directory is % burnt area mean over ~50 years. This can be replaced by a custom COG (Cloud Optimized GeoTIFF) of your choosing. See `experimentation/tile_service/raster_tiler/dynamic/data` for notebook on generating GeoTIFFs for climate data.


## How to run
0. Set up a local or remote postgres instance and load using the steps in `backend/physical-_sset/database` and `backend/physical_asset/etl`.

1. Navigate to this current directory (`/climate-risk-map/frontend/dash_leaflet`) and build the docker image.

*Note, if the docker build fails because of the build dependencies and an error about "Hash Sum Mismatch", try building on another network or VPN.*

```bash
docker build -t climate-risk-map/frontend/app .
```

2. Create a .env file, following the env.sample file. The TiTiler endpoint is meant to be
a deployed endpoint. TiTiler deployment on AWS Lambda can be done easily using [this repo and stack.](https://github.com/developmentseed/titiler-lambda-layer)

3. Run a container using the docker image we just built, using the following command

```bash
docker run -p 8050:8050 --rm --env-file .env climate-risk-map/frontend/app
```
4. Go to `http://0.0.0.0:8050/` in your broswer to see the map.

## Limitations

When experimenting, loading the infrastructure points data as a GeoJSON layer in dash-leaflet proved to crash the browser. This is likely due to the browser trying to render >100k points at once. This shows there is an inherent limitation with using GeoJSON data in the mapping application this way. 

The solution would be to use something like Tegola to serve vector tile data from the PostGIS database. Dash-Leaflet does not support vector tiles. An alternative is to enable clustering, which forces the browser not to render all the features at one time. This does not look as visually appealing as a vector tile approach would. A workaround is setting the cluster icon to transparent, giving the illusion of points generating dynamically.