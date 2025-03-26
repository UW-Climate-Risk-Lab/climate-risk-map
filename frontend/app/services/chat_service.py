"""
This module contains a service layer for handling user data analysiss.
It provides functionality for validating analysis requests, enforcing analysis limits,
and processing geographic data for export.
"""

import logging
import time
import io
import boto3
import dash_leaflet as dl
import pandas as pd
import uuid

from typing import List, Dict, Tuple
from dataclasses import dataclass

import matplotlib.pyplot as plt

import plotly.graph_objects as go

from config.settings import MAX_DOWNLOAD_AREA, AGENT_ID, AGENT_ALIAS_ID
from config.hazard_config import HazardConfig, Hazard
from config.exposure.asset import AssetConfig, Asset
from config.map_config import MapConfig, Region
from config.chat.messages import ChatMessage
from config.chat.prompts import INITIAL_PROMPT
from services.download_service import DownloadService

from utils.geo_utils import calc_bbox_area, geojson_to_pandas
from utils.file_utils import dataframe_to_csv_bytes

logger = logging.getLogger(__name__)


@dataclass
class ChatPrepConfig:
    hazard: Hazard | None
    assets: List[Asset]
    region: Region
    ssp: int
    decade: int
    month: int
    shapes: Dict
    n_clicks: int | None
    alert_message: str
    alert_message_is_open: bool
    alert_message_color: str | None
    chat_allowed: bool
    selection_config_id: int


class ChatService:

    @staticmethod
    def create_chat_config(
        shapes: Dict,
        asset_overlays: List[dl.Overlay],
        region_name: str,
        hazard_name: str,
        ssp: int,
        decade: int,
        month: int,
    ) -> ChatPrepConfig:
        """
        Creates and validates a chat analysis configuration based on user selections.

        Args:
            shapes (Dict): GeoJSON-like dictionary containing selected geographic features
            asset_overlays (List[dl.Overlay]): List of selected asset overlay layers
            region_name (str): Name of the selected geographic region
            hazard_name (str): Name of the selected climate hazard
            ssp (int): Selected Shared Socioeconomic Pathway scenario number
            decade (int): Target decade for climate projections
            month (int): Selected month for temporal filtering

        Returns:
            AnalysisConfig: Configuration object containing validated analysis parameters
                          and status messages
        """
        logger.info(
            f"Chat analysis requested: region={region_name}, hazard={hazard_name}, ssp={ssp}, month={month}, decade={decade}"
        )

        hazard = HazardConfig.get_hazard(hazard_name=hazard_name)

        assets = [
            AssetConfig.get_asset(name=asset_label) for asset_label in asset_overlays
        ]

        region = MapConfig.get_region(region_name=region_name)

        chat_config = ChatPrepConfig(
            hazard=hazard,
            assets=assets,
            region=region,
            ssp=ssp,
            month=month,
            decade=decade,
            shapes=shapes,
            n_clicks=0,
            alert_message="",
            alert_message_is_open=False,
            alert_message_color=None,
            chat_allowed=False,
            selection_config_id=None,
        )

        chat_config = ChatService._check_chat_criteria(chat_config=chat_config)

        return chat_config

    @staticmethod
    def _check_chat_criteria(
        chat_config: ChatPrepConfig,
    ) -> ChatPrepConfig:
        """
        Validates an analysis request against system constraints and user limits.

        Checks for:
        - Completeness of required parameters
        - Region availability for analysis
        - Presence of selected map areas
        - Chat count limits
        - Maximum area constraints

        Args:
            analysis_config (AnalysisConfig): The analysis configuration to validate

        Returns:
            AnalysisConfig: Updated configuration with validation results and status messages
        """
        if None in [
            chat_config.region,
            chat_config.hazard,
            chat_config.ssp,
            chat_config.month,
            chat_config.decade,
            chat_config.region,
        ]:
            chat_config.alert_message = f"To analyze data, please select all dropdowns!"
            chat_config.alert_message_is_open = True
            chat_config.alert_message_color = "danger"
            return chat_config

        if not chat_config.region.available_download:
            chat_config.alert_message = f"The region `{chat_config.region.label}` is not yet available for analysis"
            chat_config.alert_message_is_open = True
            chat_config.alert_message_color = "warning"
            return chat_config

        if chat_config.shapes is None or len(chat_config.shapes["features"]) == 0:
            chat_config.alert_message = "Please select an area on the map (Hint: Click the black square in the upper right of the map)."
            chat_config.alert_message_is_open = True
            chat_config.alert_message_color = "warning"
            return chat_config

        if calc_bbox_area(features=chat_config.shapes["features"]) > MAX_DOWNLOAD_AREA:
            chat_config.alert_message = f"Your selected area is too large for analysis"
            chat_config.alert_message_is_open = True
            chat_config.alert_message_color = "danger"
            return chat_config

        chat_config.alert_message = "Starting analysis!"
        chat_config.alert_message_is_open = True
        chat_config.alert_message_color = "success"
        chat_config.chat_allowed = True

        return chat_config

    @staticmethod
    def invoke_ai(
        user_input: str,
        session_id: str,
        showTrace: bool = False,
        endSession: bool = False,
    ) -> ChatMessage:

        try:
            runtime_agent = boto3.client(service_name="bedrock-agent-runtime")
            # Invoke the Agent - Sends a prompt for the agent to process and respond to.
            response = runtime_agent.invoke_agent(
                agentAliasId=AGENT_ALIAS_ID,  # (string) – [REQUIRED] The alias of the agent to use.
                agentId=AGENT_ID,  # (string) – [REQUIRED] The unique identifier of the agent to use.
                sessionId=session_id,  # (string) – [REQUIRED] The unique identifier of the session. Use the same value across requests to continue the same conversation.
                inputText=user_input,  # (string) - The prompt text to send the agent.
                endSession=endSession,  # (boolean) – Specifies whether to end the session with the agent or not.
                enableTrace=True,  # (boolean) – Specifies whether to turn on the trace or not to track the agent's reasoning process.
            )
            # The response of this operation contains an EventStream member.
            event_stream = response["completion"]

            # When iterated the EventStream will yield events.
            for event in event_stream:

                # chunk contains a part of an agent response
                if "chunk" in event:
                    chunk = event["chunk"]
                    if "bytes" in chunk:
                        text = chunk["bytes"].decode("utf-8")
                    else:
                        pass

                # files contains intermediate response for code interpreter if any files have been generated.
                fig = None
                if "files" in event:
                    files = event["files"]["files"]
                    for file in files:
                        name = file["name"]
                        type = file["type"]
                        bytes_data = file["bytes"]

                        # It the file is a PNG image then we can display it...
                        if type == "image/png":
                            # Display PNG image using Matplotlib
                            img = plt.imread(io.BytesIO(bytes_data))
                            fig = plt.figure(figsize=(10, 10))
        except Exception as e:
            print(f"Error: {e}")

        message = ChatMessage.create_message(role="ai", text=text, fig=fig)

        return message

    @staticmethod
    def _simulate_ai_response():
        """Placeholder response to test chat"""
        fig = go.Figure(data=go.Scatter(x=[1, 2, 3, 4], y=[10, 11, 12, 13]))
        fig.update_layout(
            title="Sample Climate Risk Analysis",
            xaxis_title="Time Period",
            yaxis_title="Risk Level",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
        )

        import random

        if random.randint(0, 1) == 1:
            fig = None

        message = ChatMessage.create_message(
            role="ai",
            text="Here is your detailed analysis cndsjinhcdkjsbncjkhdsbncjkdsnjkcndskjcjn jkdsncjkdsnjkcdn",
            fig=fig,
        )
        time.sleep(2)
        return message

    @staticmethod
    def create_session_id():
        return str(uuid.uuid4())

    @staticmethod
    def initial_agent_request(
        df: pd.DataFrame,
        session_id: str,
        showTrace: bool = False,
        endSession: bool = False,
    ) -> ChatMessage:

        try:
            runtime_agent = boto3.client(service_name="bedrock-agent-runtime")
            # Invoke the Agent - Sends a prompt for the agent to process and respond to.
            response = runtime_agent.invoke_agent(
                agentAliasId=AGENT_ALIAS_ID,  # (string) – [REQUIRED] The alias of the agent to use.
                agentId=AGENT_ID,  # (string) – [REQUIRED] The unique identifier of the agent to use.
                sessionId=session_id,  # (string) – [REQUIRED] The unique identifier of the session. Use the same value across requests to continue the same conversation.
                inputText=INITIAL_PROMPT,  # (string) - The prompt text to send the agent.
                endSession=endSession,  # (boolean) – Specifies whether to end the session with the agent or not.
                enableTrace=True,  # (boolean) – Specifies whether to turn on the trace or not to track the agent's reasoning process.
                sessionState={
                    "files": [
                        {
                            "name": "data.csv",
                            "source": {
                                "byteContent": {
                                    "data": dataframe_to_csv_bytes(df=df),
                                    "mediaType": "text/csv",
                                },
                                "sourceType": "BYTE_CONTENT",
                            },
                            "useCase": "CODE_INTERPRETER",
                        }
                    ]
                },
            )

            # The response of this operation contains an EventStream member.
            event_stream = response["completion"]

            # When iterated the EventStream will yield events.
            for event in event_stream:

                # chunk contains a part of an agent response
                if "chunk" in event:
                    chunk = event["chunk"]
                    if "bytes" in chunk:
                        text = chunk["bytes"].decode("utf-8")
                    else:
                        pass

                # files contains intermediate response for code interpreter if any files have been generated.
                fig = None
                if "files" in event:
                    files = event["files"]["files"]
                    for file in files:
                        name = file["name"]
                        type = file["type"]
                        bytes_data = file["bytes"]

                        # It the file is a PNG image then we can display it...
                        if type == "image/png":
                            # Display PNG image using Matplotlib
                            img = plt.imread(io.BytesIO(bytes_data))
                            fig = plt.figure(figsize=(10, 10))
        except Exception as e:
            print(f"Error: {e}")

        message = ChatMessage.create_message(role="ai", text=text, fig=fig)
        return message
