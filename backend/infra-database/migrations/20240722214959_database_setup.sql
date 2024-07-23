-- Add migration script here
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
CREATE ROLE ro_user WITH LOGIN PASSWORD 'mysecretpassword'
GRANT CONNECT ON DATABASE osminfra TO ro_user;
GRANT USAGE ON SCHEMA osm to ro_user;
GRANT SELECT ON ALL TABLES IN SCHEMA osm to rouser;



