-- Add migration script here
CREATE DATABASE osminfra;

CREATE EXTENSION postgis;

CREATE ROLE pgosm_flex WITH LOGIN PASSWORD 'mysecretpassword';

CREATE SCHEMA osm AUTHORIZATION pgosm_flex;

GRANT CREATE ON DATABASE osminfra
    TO pgosm_flex;
GRANT CREATE ON SCHEMA public
    TO pgosm_flex;





