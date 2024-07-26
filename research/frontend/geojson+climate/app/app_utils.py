import psycopg2
import httpx
import os

import constants

TITILER_BASE_ENDPOINT = os.environ["TITILER_BASE_ENDPOINT"]
FILE_URL = os.environ["FILE_URL"]


def query_postgis(query: str):
    # Connect to your PostGIS database
    conn = psycopg2.connect(
        "dbname='pgosm_flex_washington' user='osm_ro_user' host='localhost' password='mysecretpassword'"
    )
    cur = conn.cursor()

    # Execute the query
    cur.execute(query)
    result = cur.fetchone()[0]

    cur.close()
    conn.close()

    return result


def query_titiler(endpoint: str, params):
    r = httpx.get(url=endpoint, params=params)
    r.raise_for_status()
    return r.json()


def get_climate_min_max():
    endpoint = f"{TITILER_BASE_ENDPOINT}/cog/statistics"
    params = {"url": FILE_URL, "bidx": [1]}
    r = query_titiler(endpoint, params)

    # b1 refers to "band 1". Currently the test data is a single band
    min_climate_value = r["b1"]["min"]
    max_climate_value = r["b1"]["max"]

    return min_climate_value, max_climate_value


def get_tilejson_url():
    
    # Get min and max climate data variables to resecale
    min_climate_value, max_climate_value = get_climate_min_max()
    r = httpx.get(
        f"{TITILER_BASE_ENDPOINT}/cog/tilejson.json",
        params={
            "tileMatrixSetId": "WebMercatorQuad",
            "url": FILE_URL,
            "rescale": f"{min_climate_value},{max_climate_value}",
            "colormap_name": constants.COLORMAP,
        },
    ).json()
    return r['tiles'][0]
