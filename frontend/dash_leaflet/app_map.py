import os
import dash_leaflet as dl
import psycopg2 as pg

from typing import List

import app_config
import app_utils
import infraxclimate_api

from dash_extensions.javascript import assign

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


def get_power_grid_overlays(conn: pg.extensions.connection) -> List[dl.Overlay]:
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
    api = infraxclimate_api.infraXclimateAPI(conn=conn)

    overlays = []

    for subtype_config in app_config.POWER_GRID_LAYERS.values():

        layergroup_children = []

        # We create a separate GeoJSON component for each geom type,
        # as certain subtypes have multiple geometry types.
        # This allows a finer level of config for clustering points, which improves performance.
        # We generally want to cluster Points, and not cluster other geom types
        for geom_type in subtype_config["geom_types"]:

            params = infraxclimate_api.infraXclimateInput(
                category=subtype_config["GeoJSON"]["category"],
                osm_types=subtype_config["GeoJSON"]["osm_types"],
                osm_subtypes=subtype_config["GeoJSON"]["osm_subtypes"],
                geom_type=geom_type,
            )

            data = api.get_data(input_params=params)
            data = app_utils.create_feature_toolip(geojson=data)

            if geom_type != "Point":
                cluster = False
                clusterToLayer = None
            else:
                cluster = subtype_config["GeoJSON"]["cluster"]
                # Javascript code to create a transparent cluster icon
                clusterToLayer = app_config.TRANSPARENT_MARKER_CLUSTER

            if (subtype_config["icon"] is not None) & (geom_type == "Point"):
                pointToLayer = app_utils.create_custom_icon(
                    subtype_config["icon"]["url"]
                )
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
                    pointToLayer=pointToLayer,
                )
            )

            # This block creates icons for non-points. Above, we create icons for Point geometrys by default
            # If we want to force a non-Point geometry to display an icon, we flag the "create_points" property
            # in the config, and provide a url to the icon. This will make a database call and return the centroid to use
            # as the icon location for the given features.
            # * NOTE, performance may be an issue if there are too many features are returned
            if subtype_config["icon"] is not None:
                if (subtype_config["icon"]["create_points"]) & (geom_type != "Point"):
                    params = infraxclimate_api.infraXclimateInput(
                        category=subtype_config["GeoJSON"]["category"],
                        osm_types=subtype_config["GeoJSON"]["osm_types"],
                        osm_subtypes=subtype_config["GeoJSON"]["osm_subtypes"],
                        geom_type=geom_type,
                        centroid=True,
                    )
                    data = api.get_data(input_params=params)
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
                            pointToLayer=app_utils.create_custom_icon(
                                subtype_config["icon"]["url"]
                            ),
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


def get_feature_overlays(conn: pg.extensions.connection) -> List[dl.Overlay]:
    """Returns overlays of Geojson features

    Returns:
        List[dl.Overlay]: List of overlays for LayersControl
    """
    try:
        power_grid_features = get_power_grid_overlays(conn=conn)
    except Exception as e:
        print(str(e))

    # If more features needed in future, add on to this
    features = power_grid_features

    return features


def get_map(conn: pg.extensions.connection):

    config = app_config.MAP_COMPONENT

    base_map_layer = dl.TileLayer(
        id=config["base_map"]["id"],
        url=config["base_map"]["url"],
        attribution=config["base_map"]["attribution"],
    )

    drawn_shapes_component = dl.FeatureGroup(
        [
            dl.EditControl(
                draw=config["drawn_shapes_component"]["draw"],
                edit=config["drawn_shapes_component"]["edit"],
                id=config["drawn_shapes_component"]["id"],
            )
        ]
    )

    climate_layer = dl.TileLayer(
        url=config["base_map"]["url"], opacity=1, id="climate-tile-layer"
    )

    feature_layers = get_feature_overlays(conn=conn)

    # TODO: Make state outline dynamic, hardcoded for washington as of 08/26/2024
    state_outline_overlay = get_state_overlay(state="washington", z_index=300)

    # Default colorscale transparent while callbacks load
    color_bar = dl.Colorbar(
        id=config["color_bar"]["id"],
        width=config["color_bar"]["width"],
        colorscale=["rgba(0, 0, 0, 0)", "rgba(0, 0, 0, 0)"],
        height=config["color_bar"]["height"],
        position=config["color_bar"]["position"],
        min=0,
        max=1,
    )

    map = dl.Map(
        children=[
            base_map_layer,
            drawn_shapes_component,
            climate_layer,
            dl.LayersControl(
                id="layers-control",
                children=feature_layers,
            ),
            state_outline_overlay,
            color_bar,
        ],
        center=config["center"],
        zoom=config["zoom"],
        style=config["style"],
        id=config["id"],
        preferCanvas=config["preferCanvas"],
    )

    return map
