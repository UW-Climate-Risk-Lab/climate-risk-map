-- create_nasa_nex_fwi.sql

CREATE EXTENSION IF NOT EXISTS postgis_raster;

SET ROLE climate_user;

CREATE TABLE IF NOT EXISTS climate.nasa_nex_fwi_raster (
    id        SERIAL PRIMARY KEY,
    decade    SMALLINT,
    month     SMALLINT,
    ssp       SMALLINT,
    rast      raster
);

-- Optional index for faster spatial querying:
CREATE INDEX nasa_nex_fwi_rast_st_convexhull_idx
ON climate.nasa_nex_fwi_raster
USING GIST (ST_ConvexHull(rast));

-- Individual indexes for temporal columns
CREATE INDEX nasa_nex_fwi_rast_month_idx ON climate.nasa_nex_fwi_raster (month);
CREATE INDEX nasa_nex_fwi_rast_decade_idx ON climate.nasa_nex_fwi_raster (decade);
CREATE INDEX nasa_nex_fwi_rast_ssp_idx ON climate.nasa_nex_fwi_raster (ssp);

-- Combined index for all temporal columns
CREATE INDEX nasa_nex_fwi_rast_temporal_idx ON climate.nasa_nex_fwi_raster (decade, month, ssp);
