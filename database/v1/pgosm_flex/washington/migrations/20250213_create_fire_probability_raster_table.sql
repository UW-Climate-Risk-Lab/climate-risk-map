-- create_fire_probability_fwi.sql

CREATE EXTENSION IF NOT EXISTS postgis_raster;

SET ROLE climate_user;

CREATE TABLE IF NOT EXISTS climate.fire_probability_raster (
    id        SERIAL PRIMARY KEY,
    decade    SMALLINT,
    month     SMALLINT,
    ssp       SMALLINT,
    rast      raster
);

-- Optional index for faster spatial querying:
CREATE INDEX fire_probability_rast_st_convexhull_idx
ON climate.fire_probability_raster
USING GIST (ST_ConvexHull(rast));

-- Individual indexes for temporal columns
CREATE INDEX fire_probability_rast_month_idx ON climate.fire_probability_raster (month);
CREATE INDEX fire_probability_decade_idx ON climate.fire_probability_raster (decade);
CREATE INDEX fire_probability_ssp_idx ON climate.fire_probability_raster (ssp);

-- Combined index for all temporal columns
CREATE INDEX fire_probability_temporal_idx ON climate.fire_probability_raster (decade, month, ssp);
