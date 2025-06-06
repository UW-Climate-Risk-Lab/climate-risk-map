# PGOSM Flex ETL Service for Infrastructure Data

## Overview
Currently, the ETL Service will utilize [pgosm-flex project](https://pgosm-flex.com/) by RustProof Labs. This will allow a quick and simple pipeline to be stood up to import the required OSM data for the initial project. 

The ETL service is run via a Docker Container. The below setup is meant to be used locally for experimenting and testing.

## Set Up

0. Set up a Postgres database for the ETL pipeline. See `database/v1/migrations/init_db.sql` for a sample setup script.

1. Ensure that you have a .env file that specifies your postgres database connection details. A sample env file is provided in this directory as `env.sample`. Ram should be set lower than the total RAM on the machine to avoid overages. You can use this command to set all environment variables in your .env files.

2. Build pgosm-flex-run image. Notice version 1.0.0 is pinned to avoid breaking changes in production.

```bash
docker build -t pgosm-flex-run .
```

3. Run a container using the built image and environment variables. This will load the data into the database specified. Load time varies depending on how large the region/subregion are.
```bash
docker run --rm \
  --env-file .env
  pgosm-flex-run
```