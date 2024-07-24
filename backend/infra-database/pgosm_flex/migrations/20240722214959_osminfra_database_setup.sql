-- Creation of osminfra database
CREATE DATABASE osminfra;

\connect osminfra;

CREATE EXTENSION postgis;

CREATE ROLE pgosm_flex WITH LOGIN PASSWORD 'mysecretpassword';

CREATE SCHEMA osm AUTHORIZATION pgosm_flex;

GRANT CREATE ON DATABASE osminfra
    TO pgosm_flex;
GRANT CREATE ON SCHEMA public
    TO pgosm_flex;

-- Creates a read only user for queries
CREATE ROLE osm_ro_user WITH LOGIN PASSWORD 'mysecretpassword';
GRANT CONNECT ON DATABASE osminfra TO osm_ro_user;
GRANT USAGE ON SCHEMA osm to osm_ro_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA osm GRANT SELECT ON TABLES TO osm_ro_user;



