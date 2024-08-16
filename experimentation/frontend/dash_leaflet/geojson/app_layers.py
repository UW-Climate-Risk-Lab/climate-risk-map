import os
import dash_leaflet as dl

from typing import List
from dotenv import load_dotenv

import app_config
import app_utils
import pgosm_flex_api

from dash_extensions.javascript import assign


load_dotenv()
PG_DBNAME = os.environ["PG_DBNAME"]
PG_USER = os.environ["PG_USER"]
PG_HOST = os.environ["PG_HOST"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_PORT = os.environ["PG_PORT"]


def get_state_overlay(state: str, z_index: int) -> dl.Pane:
    """Returns a layer for a state outline

    GeoJSON component wrapped in a Pane component.
    This allows the zIndex property to be set, which
    controls what level this layer is on. We need
    these layers to be on the bottom, as we only want
    the outline to show.

    Args:
        state (str): State name (lowercase)
        z_index (int): For zIndex property (have found 300 works)

    Returns:
        dl.Pane: Returns Pane component, which is added to the Map as it's own layer
    """
    url = f"https://raw.githubusercontent.com/glynnbird/usstatesgeojson/master/{state}.geojson"
    layer = dl.Pane(
        dl.GeoJSON(
            url=url,
            style={
                "color": "#000080",
                "weight": 2,
                "fillOpacity": 0,
            },
            zoomToBoundsOnClick=True,
        ),
        id=f"{state}-outline-pane",
        name=f"{state}",
        # pointer-events as none ensures there is not interactivtity, and it is just a state outline
        # style={"pointer-events": "none"},
        style=dict(zIndex=z_index),
    )
    return layer


def get_power_grid_overlays() -> List[dl.Overlay]:
    """Generates overlays for power grid infrastructure

    An overlay in dash-leaflet is the layer that will be visible
    in the layer selection UI. An overlay consists of a LayerGroup,
    and a LayerGroup can consist of multiple layers.

    Here, each overlay represents a discrete type of power grid infrastructure.
    Some power grid features are represented across multiple geometry types
    (For example, some substations are points and some are multipolygons)

    The config file defines the available geometry types. We create a GeoJSON component
    for each geometry type, and combine those into the power grid feature's layergroup.
    This ensures that we can cluster Points when needed (for performance), and toggle
    layers on and off as needed.

    Returns:
        List[dl.Overlay]: Returns a final list of dl.Overlay components. These are added to the map's LayerControl
        component's children
    """
    api = pgosm_flex_api.OpenStreetMapDataAPI(
        dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )

    overlays = []

    for subtype_config in app_config.POWER_GRID_LAYERS.values():

        layergroup_children = []

        # We create a separate GeoJSON component for each geom type,
        # as certain subtypes have multiple geometry types.
        # This allows a finer level of config for clustering points, which improves performance.
        # We generally want to cluster Points, and not cluster other geom types
        for geom_type in subtype_config["geom_types"]:
            data = api.get_osm_data(
                categories=subtype_config["GeoJSON"]["categories"],
                osm_types=subtype_config["GeoJSON"]["osm_types"],
                osm_subtypes=subtype_config["GeoJSON"]["osm_subtypes"],
                geom_type=geom_type,
            )
            data = app_utils.create_feature_toolip(geojson=data)

            if geom_type != "Point":
                cluster = False
                clusterToLayer = None
            else:
                cluster = subtype_config["GeoJSON"]["cluster"]
                # Javascript code to create a transparent cluster icon
                clusterToLayer = app_config.TRANSPARENT_MARKER_CLUSTER

            if (subtype_config["icon"] is not None) & (geom_type=="Point"):
                pointToLayer = app_utils.create_custom_icon(subtype_config["icon"]["url"])
            else:
                pointToLayer = None

            layergroup_children.append(
                dl.GeoJSON(
                    id=subtype_config["GeoJSON"]["id"] + f"-{geom_type}",
                    data=data,
                    hoverStyle=subtype_config["GeoJSON"]["hoverStyle"],
                    style=subtype_config["GeoJSON"]["style"],
                    cluster=cluster,
                    clusterToLayer=clusterToLayer,
                    superClusterOptions=subtype_config["GeoJSON"][
                        "superClusterOptions"
                    ],
                    pointToLayer=pointToLayer
                )
            )

            # This block creates icons for non-points. Above, we create icons for Point geometrys by default
            # If we want to force a non-Point geometry to display an icon, we flag the "create_points" property
            # in the config, and provide a url to the icon. This will make a database call and return the centroid to use
            # as the icon location for the given features.
            # * NOTE, performance may be an issue if there are too manyt features returned
            if (subtype_config["icon"] is not None):
                if (subtype_config["icon"]["create_points"]) & (geom_type != "Point"):
                    data = api.get_osm_data(
                        categories=subtype_config["GeoJSON"]["categories"],
                        osm_types=subtype_config["GeoJSON"]["osm_types"],
                        osm_subtypes=subtype_config["GeoJSON"]["osm_subtypes"],
                        geom_type=geom_type,
                        centroid=True,
                    )
                    data = app_utils.create_feature_toolip(geojson=data)
                    layergroup_children.append(
                        dl.GeoJSON(
                            id=subtype_config["GeoJSON"]["id"] + f"-icon",
                            data=data,
                            hoverStyle=subtype_config["GeoJSON"]["hoverStyle"],
                            style=subtype_config["GeoJSON"]["style"],
                            cluster=cluster,
                            clusterToLayer=clusterToLayer,
                            superClusterOptions=subtype_config["GeoJSON"][
                                "superClusterOptions"
                            ],
                            pointToLayer=app_utils.create_custom_icon(subtype_config["icon"]["url"])
                        )
                    )

        overlay = dl.Overlay(
            id=subtype_config["Overlay"]["id"],
            name=subtype_config["Overlay"]["name"],
            checked=subtype_config["Overlay"]["checked"],
            children=[dl.LayerGroup(children=layergroup_children)],
        )

        overlays.append(overlay)

    return overlays
