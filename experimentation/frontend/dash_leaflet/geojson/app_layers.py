import os
import dash_leaflet as dl

from typing import List
from dotenv import load_dotenv

import app_config
import pgosm_flex_api


load_dotenv()
PG_DBNAME = os.environ["PG_DBNAME"]
PG_USER = os.environ["PG_USER"]
PG_HOST = os.environ["PG_HOST"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_PORT = os.environ["PG_PORT"]


def get_infrastucture_overlays() -> List[dl.Overlay]:
    api = pgosm_flex_api.OpenStreetMapDataAPI(
        dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )

    overlays = []

    for key, value in app_config.INFRASTRUCTURE_LAYERS.items():
        data = api.get_osm_data(
            categories=value["GeoJSON"]["categories"],
            osm_types=value["GeoJSON"]["osm_types"],
            osm_subtypes=value["GeoJSON"]["osm_subtypes"],
        )
        overlay = dl.Overlay(
            id=value["Overlay"]["id"],
            name=value["Overlay"]["name"],
            checked=value["Overlay"]["checked"],
            children=[
                dl.LayerGroup(
                    children=dl.GeoJSON(
                        id=value["GeoJSON"]["id"],
                        data=data,
                        hoverStyle=value["GeoJSON"]["hoverStyle"],
                        style=value["GeoJSON"]["style"],
                        cluster=value["GeoJSON"]["cluster"],
                        superClusterOptions=value["GeoJSON"]["superClusterOptions"],
                    )
                )
            ],
        )
        overlays.append(overlay)
    return overlays
