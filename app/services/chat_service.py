"""
This module contains a service layer for handling user data analysiss.
It provides functionality for validating analysis requests, enforcing analysis limits,
and processing geographic data for export.
"""

import logging
import boto3
import dash_leaflet as dl
import pandas as pd
import uuid

from typing import List, Dict, Tuple, Optional

from config.settings import (
    MAX_DOWNLOAD_AREA,
    ENABLE_AI_ANALYSIS,
    AGENT_ID,
    AGENT_ALIAS_ID,
    AGENT_REGION
)
from config.hazard_config import HazardConfig
from config.exposure import get_asset
from config.map_config import MapConfig
from config.chat.messages import ChatMessage
from config.chat.prompts import INITIAL_PROMPT

from utils.geo_utils import calc_bbox_area
from utils.file_utils import dataframe_to_csv_bytes

logger = logging.getLogger(__name__)


class ChatService:
    @staticmethod
    def _invoke_bedrock_agent(
        session_id: str,
        input_text: str,
        end_session: bool = False,
        session_state: Optional[Dict] = None,
    ) -> str:
        """
        Helper function to invoke the Bedrock Agent and process the response stream.

        Args:
            session_id: The active session ID.
            input_text: The text prompt for the agent.
            end_session: Whether to end the session.
            session_state: Optional session state (e.g., for initial file upload).

        Returns:
            str
            - The aggregated text response from the agent (str or None).
        """
        logger.info(
            f"Invoking Bedrock Agent for session {session_id}. Input starts with: '{input_text[:10]}...'"
        )
        text_response = ""

        try:
            # runtime_agent = ChatService._get_bedrock_runtime_client() # If using class method client
            runtime_agent = boto3.client(
                service_name="bedrock-agent-runtime",
                region_name=AGENT_REGION
            )  # Instantiate per call

            invoke_params = {
                "agentAliasId": AGENT_ALIAS_ID,
                "agentId": AGENT_ID,
                "sessionId": session_id,
                "inputText": input_text,
                "endSession": end_session,
                "enableTrace": True,  # Consider making this configurable
            }
            if session_state:
                invoke_params["sessionState"] = session_state

            response = runtime_agent.invoke_agent(**invoke_params)

            event_stream = response.get("completion")
            if not event_stream:
                logger.warning(
                    f"No 'completion' event stream found in Bedrock response for session {session_id}."
                )
                return None, None

            for event in event_stream:
                if "chunk" in event:
                    chunk = event["chunk"]
                    if "bytes" in chunk:
                        text_response += chunk["bytes"].decode("utf-8")
                    else:
                        # Handle other potential chunk types if necessary
                        logger.debug(f"Received non-bytes chunk: {chunk}")
                elif "trace" in event:
                    # You could log trace details here if needed for debugging
                    logger.debug(
                        f"Trace event received for session {session_id}"
                    )  # Example
                elif "files" in event:
                    # Placeholder for file/image handling from Bedrock Agent Code Interpreter
                    # This part needs implementation based on expected file output
                    logger.info(
                        f"Received 'files' event for session {session_id}: {event['files']}"
                    )
                    # Example (needs refinement based on actual use case):
                    # files = event["files"].get("files", [])
                    # for file in files:
                    #     name = file.get("name")
                    #     type = file.get("type")
                    #     bytes_data = file.get("bytes")
                    #     if type == "image/png" and bytes_data:
                    #         # Decide how to handle/store/return image data
                    #         # Maybe return the bytes directly or process into a figure object
                    #         logger.info(f"Received PNG image '{name}'")
                    #         # Example: Store bytes in figure_data (adjust as needed)
                    #         # figure_data = {"type": "image/png", "bytes": bytes_data, "name": name}
                    #         pass # Implement actual handling
                # Add handling for other event types like 'returnControl' if needed

            logger.info(
                f"Agent invocation successful for session {session_id}. Response length: {len(text_response)}"
            )
            return text_response

        except runtime_agent.exceptions.ValidationException as e:
            logger.error(
                f"Bedrock Agent Validation Error for session {session_id}: {e}. Input: '{input_text[:100]}...'"
            )
            return f"Agent Error: Input validation failed. {e}", None
        except runtime_agent.exceptions.ConflictException as e:
            logger.error(
                f"Bedrock Agent Conflict Error for session {session_id}: {e}. Maybe session issue?"
            )
            # You might want to try creating a new session or inform the user
            return (
                f"Agent Error: Session conflict. Please try closing and reopening the chat. {e}",
                None,
            )
        except Exception as e:
            logger.error(f"Error invoking Bedrock Agent for session {session_id}: {e}")
            return (
                f"An unexpected error occurred while communicating with the AI agent: {e}",
                None,
            )

    @staticmethod
    def check_chat_criteria(
        shapes: Optional[Dict],
        asset_overlays: List[dl.Overlay],
        region_name: Optional[str],
        hazard_name: Optional[str],
        ssp: Optional[int],
        decade: Optional[int],
        month: Optional[int],
    ) -> Tuple[bool, str]:
        """
        Validates based on user selections,
        determining if a chat session is allowed to start.

        Args:
            shapes: GeoJSON-like dictionary containing selected geographic features.
            asset_overlays: List of selected asset overlay *labels* or identifiers.
            region_name: Name of the selected geographic region.
            hazard_name: Name of the selected climate hazard.
            ssp: Selected Shared Socioeconomic Pathway scenario number.
            decade: Target decade for climate projections.
            month: Selected month for temporal filtering.

        Returns:
            Tuple[bool, str]: Bool indicates if chat can be initiated, str contains text to display to user reason
        """
        logger.info(
            f"Validating chat analysis request: region={region_name}, hazard={hazard_name}, "
            f"ssp={ssp}, month={month}, decade={decade}, assets={asset_overlays}"
        )
        # Initialize return values
        chat_allowed = False
        chat_allowed_message = ""

        # Use .get() methods to handle potential None values gracefully
        hazard = (
            HazardConfig.get_hazard(hazard_name=hazard_name) if hazard_name else None
        )
        region = MapConfig.get_region(region_name=region_name) if region_name else None
        assets = [
            get_asset(name=asset_label)
            for asset_label in asset_overlays
            if get_asset(name=asset_label)
        ]

        # --- Validation Checks ---
        missing_params = []
        if not region:
            missing_params.append("Region")
        if not hazard:
            missing_params.append("Hazard Indicator")
        if not ssp:
            missing_params.append("SSP Scenario")
        if not month:
            missing_params.append("Month")
        if not decade:
            missing_params.append("Decade")

        if not ENABLE_AI_ANALYSIS:
            chat_allowed_message = "AI features are not currently enabled at this time."
            chat_allowed = False
            return chat_allowed, chat_allowed_message

        if missing_params:
            chat_allowed_message = (
                "To start analysis, please select: {', '.join(missing_params)}."
            )
            chat_allowed = False
            return chat_allowed, chat_allowed_message

        if not any(assets):
            chat_allowed_message = "Please select your asset overlays."
            chat_allowed = False
            return chat_allowed, chat_allowed_message

        if not region.available_download:  # Check attribute exists before access
            chat_allowed_message = (
                "The region '{region.label}' is not yet available for analysis."
            )
            chat_allowed = False
            return chat_allowed, chat_allowed_message

        if shapes is None or not shapes.get("features"):  # Safer check for features
            chat_allowed_message = (
                "Please select an area on the map (Hint: Use the drawing tools)."
            )
            chat_allowed = False
            return chat_allowed, chat_allowed_message

        # Perform area calculation only if shapes are valid
        try:
            area = calc_bbox_area(features=shapes["features"])
            if area > MAX_DOWNLOAD_AREA:
                chat_allowed_message = f"Your selected area ({area:.2f} sq km) is too large for analysis (Max: {MAX_DOWNLOAD_AREA} sq km)."
                chat_allowed = False
                return chat_allowed, chat_allowed_message
        except Exception as e:
            logger.error(f"Error calculating area for shapes: {e}")
            chat_allowed_message = (
                "Could not calculate the area of the selected shapes."
            )
            chat_allowed = False
            return chat_allowed, chat_allowed_message

        # If all checks pass
        chat_allowed_message = "Configuration valid. Ready to start analysis!"
        chat_allowed = True
        logger.info("Chat preparation successful. Chat allowed.")

        return chat_allowed, chat_allowed_message

    @staticmethod
    def invoke_ai_agent(
        user_input: str,
        session_id: str,
    ) -> ChatMessage:
        """
        Sends user input to the Bedrock agent for an ongoing session.

        Args:
            user_input: The user's message text.
            session_id: The active session ID.

        Returns:
            ChatMessage: The AI's response message.
        """
        if not user_input:
            logger.warning("invoke_ai_agent called with empty input.")
            return ChatMessage.create_message(
                role="ai", text="I didn't receive any input. Please try again."
            )

        text = ChatService._invoke_bedrock_agent(
            session_id=session_id,
            input_text=user_input,
            end_session=False,  # Typically don't end session here
        )

        if text is None:  # Handle case where agent invocation failed completely
            text = "Sorry, I encountered an error and couldn't process your request."

        # Create message even if text indicates an error from the agent helper
        message = ChatMessage.create_message(role="ai", text=text)
        return message

    @staticmethod
    def start_ai_session(
        df: pd.DataFrame,  # The pre-processed data for the selected criteria
        initial_prompt: str = INITIAL_PROMPT,
    ) -> Tuple[Optional[str], Optional[ChatMessage]]:
        """
        Starts a new Bedrock agent session, uploads initial data, and sends the initial prompt.

        Args:
            df: DataFrame containing the data for analysis.
            initial_prompt: The initial system prompt to guide the agent.

        Returns:
            A tuple containing:
            - The new session ID (str or None if failed).
            - The initial AI response message (ChatMessage or None if failed).
        """
        session_id = str(uuid.uuid4())
        logger.info(f"Starting new AI session: {session_id}")

        try:
            csv_bytes = dataframe_to_csv_bytes(df=df)
            if not csv_bytes:
                logger.error("Failed to convert DataFrame to CSV bytes.")
                return None, None
        except Exception as e:
            logger.error(
                f"Error converting DataFrame to CSV for session {session_id}: {e}"
            )
            return None, None

        session_state = {
            "files": [
                {
                    "name": "data.csv",  # Keep filename simple
                    "source": {
                        "byteContent": {
                            "data": csv_bytes,  # Use the generated bytes
                            "mediaType": "text/csv",
                        },
                        "sourceType": "BYTE_CONTENT",
                    },
                    "useCase": "CODE_INTERPRETER",
                }
            ]
        }

        text = ChatService._invoke_bedrock_agent(
            session_id=session_id,
            input_text=initial_prompt,
            session_state=session_state,
            end_session=False,
        )

        if text is None:  # Handle case where initial agent invocation failed
            logger.error(
                f"Initial agent invocation failed for new session {session_id}."
            )
            # Decide if we should still return the session ID or None
            return session_id, ChatMessage.create_message(
                role="ai",
                text="Sorry, I couldn't initialize the analysis session.",
                fig=None,
            )

        message = ChatMessage.create_message(role="ai", text=text)
        return session_id, message
