# PGOSM Flex ETL Service for Infrastructure Data

## Overview
Currently, the Infrastructure ETL Service will utilize [pgosm-flex project](https://pgosm-flex.com/) by RustProof Labs. This will allow a quick and simple pipeline to be stood up to import the required OSM data for the initial project. 

The ETL service is run via a Docker Container.

## Set Up

 1. Ensure that you have a .env file that specifies your postgres database connection details. A sample env file is provided in this directory as `env.sample`. Ram should be set lower than the total RAM on the machine to avoid overages.



```bash
POSTGRES_USER=pgosm_flex
POSTGRES_PASSWORD=mysecretpassword
POSTGRES_HOST=localhost
POSTGRES_DB=osminfra
POSTGRES_PORT=5432
RAM=8
```

 2. Create a directory where the raw OSM data will be downloaded to. This will be mounted to the container to archive .pbf files.
 
 ```bash
 mkdir ~/pgosm-data
 ```

 3. You can use this command to set all environment variables in your .env files.

```bash
source $(pwd)/backend/infra-etl-service/pgosm_flex/.env
```
4. Run the pgosm-flex image, noting to pass in appropiate volume mounts and environement variables. Notice version 1.0.0 is pinned to avoid breaking changes in production.

```bash
docker run --name pgosm -d --rm \
  -v ~/pgosm-data:/app/output \
  -v /etc/localtime:/etc/localtime:ro \
  -v $(pwd)/backend/infra-etl-service/pgosm_flex/custom-layerset:/custom-layerset \
  -e POSTGRES_USER=$POSTGRES_USER \
  -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
  -e POSTGRES_HOST=$POSTGRES_HOST \
  -e POSTGRES_DB=$POSTGRES_DB \
  -e POSTGRES_PORT=$POSTGRES_PORT \
  -p 5433:5432 \
  -d rustprooflabs/pgosm-flex:1.0.0
```

5. Once the container is running, use this command to execute the script that runs the ETL process. Replication will check if data has already been imported and if so, will perform a data update only. 
```bash
docker exec -it \
    pgosm python3 docker/pgosm_flex.py \
    --layerset=infrastructure \
    --layerset-path=/custom-layerset/ \
    --ram=$RAM \
    --region=north-america \
    --subregion=us-west \
    --srid=3857 \
    --replication
    
```