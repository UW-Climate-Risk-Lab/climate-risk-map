# Dash-Leaflet Frontend with GeoJSON Layer of POI

This directory contains a simple dash app, `app.py`, that queries a PostGIS database to get a GeoJSON layer for the map. This is specifically looking at power grid infrastructure data layer.

Note, when running, the infrastructure_points query returned too large of a GeoJSON, and crashed the app. The alternative to this would be to create a vector tile server to return tiles instead of GeoJSON data. However, dash-leaflet does not at this time support vector tiles. This may be able to be done with some custom javascript code.