import logging
import time

from dash import Input, Output, no_update, State, callback_context

from config.settings import MAX_CHATS
from config.map_config import MapConfig
from config.chat.messages import ChatMessage

from services.download_service import DownloadService
from services.chat_service import ChatService
from utils.error_utils import handle_callback_error
from utils.misc_utils import generate_robust_hash

logger = logging.getLogger(__name__)


def register_chat_callbacks(app):
    """Register callbacks for AI analysis modal and chat interface

    Args:
        app: Dash application instance
    """

    @app.callback(
        Output("chat-modal", "is_open"),
        [Input("analysis-btn", "n_clicks"), Input("chat-modal-close", "n_clicks")],
        [State("chat-modal", "is_open"), State("chat-allowed", "data")],
        prevent_initial_call=True,
    )
    def toggle_modal(open_clicks, close_clicks, is_open, chat_allowed):
        """Toggle the visibility of the AI analysis modal"""
        ctx = callback_context
        if not ctx.triggered:
            return no_update

        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

        if trigger_id == "analysis-btn" and chat_allowed:
            return True
        elif trigger_id == "chat-modal-close" and close_clicks:
            return False
        return is_open

    @app.callback(
        [
            Output("chat-messages", "children", allow_duplicate=True),
            Output("chat-user-input", "value"),
            Output("chat-counter", "data"),
            Output("chat-alert-message", "is_open"),
            Output("chat-alert-message", "children"),
            Output("chat-alert-message", "color"),
            Output("trigger-ai-response-store", "data", allow_duplicate=True),
        ],
        [
            Input("chat-send-button", "n_clicks"),
            Input("chat-user-input", "n_submit"),
        ],
        [
            State("chat-user-input", "value"),
            State("chat-messages", "children"),
            State("chat-counter", "data"),
        ],
        prevent_initial_call=True,
    )
    def send_user_message(
        send_clicks, enter_pressed, user_input, chat_messages, chat_count
    ):
        """Handle sending user messages and adding them to the chat window"""

        if not isinstance(chat_messages, list):
            chat_messages = [chat_messages]

        if (not send_clicks and not enter_pressed) or not user_input:
            return (no_update,) * 6

        if chat_count >= MAX_CHATS:  # Check before incrementing/adding
            logger.warning(f"Max chat limit ({MAX_CHATS}) reached.")
            return (
                no_update,
                user_input,
                no_update,
                True,  # Show alert
                f"You have reached the maximum of {MAX_CHATS} messages.",
                "warning",
                no_update,  # Don't trigger AI
            )

        logger.info("User sending message.")
        user_message = ChatMessage.create_message(role="user", text=user_input)
        chat_messages.append(user_message)
        new_chat_count = chat_count + 1
        trigger_ai_value = time.time()  # Simple trigger value change

        return (
            chat_messages,
            "",  # Clear input field
            new_chat_count,
            False,  # Close alert
            no_update,
            no_update,
            trigger_ai_value,  # Trigger the AI response callback
        )

    @app.callback(
        [
            Output("chat-allowed", "data"),
            Output("analysis-btn", "n_clicks"),
            Output("alert-message", "children", allow_duplicate=True),
            Output("alert-message", "is_open", allow_duplicate=True),
            Output("alert-message", "color", allow_duplicate=True),
        ],
        [
            Input("analysis-btn", "n_clicks"),
        ],
        [
            State(MapConfig.BASE_MAP_COMPONENT["drawn_shapes_layer"]["id"], "geojson"),
            State(MapConfig.BASE_MAP_COMPONENT["asset_layer"]["id"], "overlays"),
            State("region-select-dropdown", "value"),
            State("hazard-indicator-dropdown", "value"),
            State("ssp-dropdown", "value"),
            State("decade-slider", "value"),
            State("month-slider", "value"),
        ],
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=5)
    def prep_chat_validation(
        n_clicks,
        shapes,
        selected_assets,
        selected_region,
        selected_hazard,
        selected_ssp,
        selected_decade,
        selected_month,
    ):
        if n_clicks > 0:
            chat_allowed, chat_allowed_message = ChatService.check_chat_criteria(
                shapes=shapes,
                asset_overlays=selected_assets,
                region_name=selected_region,
                hazard_name=selected_hazard,
                ssp=selected_ssp,
                decade=selected_decade,
                month=selected_month,
            )

            if chat_allowed:
                return (
                    chat_allowed,
                    0,
                    chat_allowed_message,
                    True,
                    "success",
                )
            else:
                return (
                    chat_allowed,
                    0,
                    chat_allowed_message,
                    True,
                    "danger",
                )

        return no_update, 0, no_update, no_update, no_update

    @app.callback(
        [
            Output("chat-messages", "children"),
            Output("chat-loading-placeholder", "children"),
            Output("agent-session-id", "data"),
            Output("trigger-ai-response-store", "data", allow_duplicate=True),
        ],
        [Input("chat-modal", "is_open"), 
         Input("trigger-ai-response-store", "data"),
         Input("new-user-selection", "data")],
        [
            State("chat-allowed", "data"),
            State("chat-messages", "children"),
            State("agent-session-id", "data"),
            State(MapConfig.BASE_MAP_COMPONENT["drawn_shapes_layer"]["id"], "geojson"),
            State(MapConfig.BASE_MAP_COMPONENT["asset_layer"]["id"], "overlays"),
            State("region-select-dropdown", "value"),
            State("hazard-indicator-dropdown", "value"),
            State("ssp-dropdown", "value"),
            State("decade-slider", "value"),
            State("month-slider", "value"),
        ],
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=4)
    def get_ai_response(
        modal_is_open,
        trigger_ai_ts,
        new_user_selection,
        chat_allowed,
        current_messages,
        session_id,
        shapes,
        asset_overlays,
        selected_region,
        selected_hazard,
        selected_ssp,
        selected_decade,
        selected_month,
    ):
        ctx = callback_context
        if not ctx.triggered_id:
            return no_update

        triggered_input_id = ctx.triggered_id

        if not isinstance(current_messages, list):
            current_messages = [current_messages] if current_messages else []

        # --- Scenario 1: Initial AI message when modal opens ---
        # Check if modal opened, chat is allowed, and no messages exist yet (or only initial system message)
        if (
            triggered_input_id == "chat-modal"
            and modal_is_open
            and chat_allowed
            and new_user_selection
        ):
            logger.info(
                "Modal opened and chat allowed. Requesting initial AI analysis."
            )

            # Ensure asset_overlays is a list
            if asset_overlays is None:
                asset_overlays = []
            elif not isinstance(asset_overlays, list):
                asset_overlays = [asset_overlays]

            # 1. Fetch data using DownloadService (assuming it returns a DataFrame)
            try:
                logger.info("Fetching data for initial analysis...")
                # Make sure DownloadService handles potential None values for parameters if needed
                df = DownloadService.get_download(
                    shapes=shapes,
                    asset_overlays=asset_overlays,
                    region_name=selected_region,
                    hazard_name=selected_hazard,
                    ssp=selected_ssp,
                    decade=selected_decade,
                    month=selected_month,
                )
                if df is None or df.empty:
                    logger.warning("Data fetch returned empty or None DataFrame.")
                    # Handle appropriately - maybe show error message in chat?
                    error_message = ChatMessage.create_message(
                        role="ai",
                        text="Sorry, I could not retrieve the required data for analysis.",
                    )
                    return (
                        current_messages + [error_message],
                        "",
                        no_update,
                        no_update
                    )  # Keep session_id as None
            except Exception as e:
                logger.error(
                    f"Error fetching data via DownloadService: {e}", exc_info=True
                )
                error_message = ChatMessage.create_message(
                    role="ai", text=f"Sorry, an error occurred while fetching data: {e}"
                )
                return (
                    current_messages + [error_message],
                    "",
                    no_update,
                    no_update,
                )

            # 2. Start AI Session
            logger.info("Starting AI session with Bedrock Agent...")
            new_session_id, initial_message = ChatService.start_ai_session(df=df)

            if not new_session_id or not initial_message:
                logger.error("Failed to start AI session.")
                # Handle failure - maybe show error message
                error_message = ChatMessage.create_message(
                    role="ai",
                    text="Sorry, I could not initialize the analysis session with the AI.",
                )
                return (
                    current_messages + [error_message],
                    "",
                    None,
                    no_update,
                    no_update,
                )  # Reset session ID

            logger.info(
                f"AI session started ({new_session_id}). Received initial response."
            )
            updated_messages = current_messages + [initial_message]
            return (
                updated_messages,
                "",
                new_session_id,
                no_update,
            )  # Update messages, clear loading, set session ID

        # --- Scenario 2: Subsequent AI response triggered by user message ---
        # Check if triggered by the AI trigger store and there is an active session_id
        elif triggered_input_id == "trigger-ai-response-store" and session_id:
            # Check if the last message was from the user
            if (
                current_messages
                and current_messages[-1]["props"]["className"]
                == "user-message-container"
            ):
                last_user_message_text = current_messages[-1]["props"]["children"][0][
                    "props"
                ]["children"]  # Get text from the last message object
                logger.info(
                    f"User message sent. Requesting subsequent AI response for session {session_id}."
                )

                if not last_user_message_text:
                    logger.warning("Last user message text is empty. Skipping AI call.")
                    return no_update  # Or maybe return an error message?

                # Invoke AI Agent
                ai_response_message = ChatService.invoke_ai_agent(
                    user_input=last_user_message_text, session_id=session_id
                )
                logger.info("Received subsequent AI response.")
                updated_messages = current_messages + [ai_response_message]

                return (
                    updated_messages,
                    "",
                    session_id,
                    no_update,
                )  # Update messages, clear loading, keep session ID

            else:
                # This case might happen if the trigger fires unexpectedly
                logger.warning(
                    "AI response triggered, but last message was not from user or chat is empty. No action taken."
                )
                return (
                    no_update,
                    no_update,
                    no_update,
                    no_update,
                )  # No changes
        else:
            # Other trigger conditions (e.g., modal closing, initial load) - do nothing for AI response
            logger.debug(
                f"AI interaction callback triggered by {triggered_input_id}, but conditions not met for AI call."
            )
            return no_update, no_update, no_update, no_update

    @app.callback(
        [
            Output("chat-messages", "children", allow_duplicate=True),
            Output("new-user-selection", "data"),
            Output("chat-selection-hash", "data")
        ],
        [Input("chat-modal", "is_open")],
        [   
            State("chat-selection-hash", "data"),
            State("chat-messages", "children"),
            State(MapConfig.BASE_MAP_COMPONENT["drawn_shapes_layer"]["id"], "geojson"),
            State(MapConfig.BASE_MAP_COMPONENT["asset_layer"]["id"], "overlays"),
            State("region-select-dropdown", "value"),
            State("hazard-indicator-dropdown", "value"),
            State("ssp-dropdown", "value"),
            State("decade-slider", "value"),
            State("month-slider", "value"),
        ],
        prevent_initial_call=True,
    )
    def clear_chat_window(modal_is_open,
        previous_selection_hash,
        current_messages,
        shapes,
        asset_overlays,
        selected_region,
        selected_hazard,
        selected_ssp,
        selected_decade,
        selected_month,):

        selected_hash = generate_robust_hash(
            shapes, asset_overlays, selected_region, selected_hazard, selected_decade, selected_month, selected_ssp
        )
        new_selection = selected_hash != previous_selection_hash

        if new_selection:
            current_messages = [current_messages[0]]
        
        return (current_messages, new_selection, selected_hash)


