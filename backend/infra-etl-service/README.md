# ETL Service for Infrastructure Data

## Overview

## Set Up

 1. Ensure that you have a .env file that specifies your postgres database connection details
 2. Run the following in order from the root of this repository
 
 ```bash
 mkdir ~/pgosm-data
 ```

```bash
source $(pwd)/backend/infra-etl-service/.pgosm-db-osminfra
```

```bash
docker run --name pgosm -d --rm \
  -v ~/pgosm-data:/app/output \
  -v /etc/localtime:/etc/localtime:ro \
  -v $(pwd)/backend/infra-etl-service/custom-layerset:/custom-layerset \
  -e POSTGRES_USER=$POSTGRES_USER \
  -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
  -e POSTGRES_HOST=$POSTGRES_HOST \
  -e POSTGRES_DB=$POSTGRES_DB \
  -e POSTGRES_PORT=$POSTGRES_PORT \
  -p 5433:5432 \
  -d rustprooflabs/pgosm-flex:1.0.0
```

```bash
docker exec -it \
    pgosm python3 docker/pgosm_flex.py \
    --layerset=infrastructure \
    --layerset-path=/custom-layerset/ \
    --ram=$RAM \
    --region=north-america \
    --subregion=us-west \
    --replication
```