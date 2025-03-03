import dash_leaflet as dl
import psycopg2 as pg

from typing import List

import app_config
import app_utils
import infraxclimate_api

from dash import html


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

        params = infraxclimate_api.infraXclimateInput(
            category=subtype_config["GeoJSON"]["category"],
            osm_types=subtype_config["GeoJSON"]["osm_types"],
            osm_subtypes=subtype_config["GeoJSON"]["osm_subtypes"],
        )

        data = api.get_data(input_params=params)
        data = app_utils.create_feature_toolip(geojson=data)

        if subtype_config["GeoJSON"]["cluster"]:
            data = app_utils.convert_geojson_feature_collection_to_points(geojson=data, preserve_types=["LineString"])
            cluster = subtype_config["GeoJSON"]["cluster"]
            clusterToLayer = app_config.TRANSPARENT_MARKER_CLUSTER
            superClusterOptions = subtype_config["GeoJSON"]["superClusterOptions"]

        else:
            cluster = False
            clusterToLayer = None
            superClusterOptions = False

        if subtype_config["icon"] is not None:
            pointToLayer = app_utils.create_custom_icon(subtype_config["icon"]["url"])
        else:
            pointToLayer = None

        layergroup_children.append(
            dl.GeoJSON(
                id=subtype_config["GeoJSON"]["id"],
                data=data,
                hoverStyle=subtype_config["GeoJSON"]["hoverStyle"],
                style=subtype_config["GeoJSON"]["style"],
                cluster=cluster,
                clusterToLayer=clusterToLayer,
                superClusterOptions=superClusterOptions,
                pointToLayer=pointToLayer,
            )
        )

        overlay = dl.Overlay(
            id=subtype_config["Overlay"]["id"],
            name=subtype_config["Overlay"]["name"],
            checked=subtype_config["Overlay"]["checked"],
            children=[dl.LayerGroup(children=layergroup_children)],
        )

        overlays.append(overlay)

    del api
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
    color_bar = html.Div(
        id=config["color_bar"]["id"] + "-div", style={"display": "none"}, children=[]
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
