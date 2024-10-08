import infraxclimate_api
import os
import psycopg2
import time
import app_utils

PG_DBNAME = os.environ["PG_DBNAME"]
PG_USER = os.environ["PG_USER"]
PG_HOST = os.environ["PG_HOST"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_PORT = os.environ["PG_PORT"]

if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )

    api = infraxclimate_api.infraXclimateAPI(conn=conn)
    # Test bbox selection, should return 2 power plants
    bbox = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "type": "rectangle",
                    "_bounds": [
                        {"lat": 47.61402337357123, "lng": -119.32662963867189},
                        {"lat": 47.62651702078168, "lng": -119.27650451660158},
                    ],
                    "_leaflet_id": 11228,
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-119.32662963867189, 47.61402337357123],
                            [-119.32662963867189, 47.62651702078168],
                            [-119.27650451660158, 47.62651702078168],
                            [-119.27650451660158, 47.61402337357123],
                            [-119.32662963867189, 47.61402337357123],
                        ]
                    ],
                },
            },
            {
                "type": "Feature",
                "properties": {
                    "type": "rectangle",
                    "_bounds": [
                        {"lat": 47.49541671416695, "lng": -119.30191040039064},
                        {"lat": 47.50747495167563, "lng": -119.27444458007814},
                    ],
                    "_leaflet_id": 11242,
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-119.30191040039064, 47.49541671416695],
                            [-119.30191040039064, 47.50747495167563],
                            [-119.27444458007814, 47.50747495167563],
                            [-119.27444458007814, 47.49541671416695],
                            [-119.30191040039064, 47.49541671416695],
                        ]
                    ],
                },
            },
        ],
    }
    start_time = time.time()
    params = infraxclimate_api.infraXclimateInput(
        category="infrastructure",
        osm_types=["power"],
        osm_subtypes=["line"],
        bbox=None,
        county=True,
        city=True,
        climate_variable="burntFractionAll",
        climate_decade=[2060, 2070],
        climate_month=[8, 9],
        climate_ssp=126,
        climate_metadata=True,
    )
    data = api.get_data(input_params=params
    )
    end_time = time.time()
    print(f"Execution time: {end_time - start_time} seconds")
    print(data)
