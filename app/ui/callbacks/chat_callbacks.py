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

LOADING_STEPS = [
    "Querying physical assets...",
    "Transferring data to AI Agent...",
    "Prepping Analysis..."
]

THINKING_STEPS = [
    "Retrieving climate data...",
    "Processing regional parameters...",
    "Analyzing hazard indicators...",
    "Evaluating infrastructure assets...",
    "Calculating risk exposure...",
    "Generating insights...",
]


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
            Output("climate-thinking-indicator", "style"),
            Output("thinking-interval", "max_intervals"),
            Output("thinking-progress", "value"),
            Output("thinking-step-indicator", "children"),
            Output("thinking-status-text", "children"),
        ],
        [
            Input("loading-info-store", "data"),
            Input("thinking-interval", "n_intervals"),
        ],
        [State("animation-info-store", "data")],
        prevent_initial_call=True,
    )
    def manage_loading_indicator_ui(loading_info_data, n_intervals, animation_info_data):
        hide_style = {
            "display": "none",
            "position": "absolute",
            "top": "50%",
            "left": "50%",
            "transform": "translate(-50%, -50%)",
            "zIndex": "1500",
            "borderRadius": "10px",
            "boxShadow": "0 4px 12px rgba(0,0,0,0.25)",
            "backgroundColor": "rgba(255, 255, 255, 0.95)",
            "maxWidth": "60%",
        }
        open_style = {
            "display": "flex",
            "flexDirection": "column",
            "alignItems": "center",
            "position": "absolute",
            "top": "50%",
            "left": "50%",
            "transform": "translate(-50%, -50%)",
            "zIndex": "1500",
            "borderRadius": "10px",
            "boxShadow": "0 4px 12px rgba(0,0,0,0.25)",
            "backgroundColor": "rgba(255, 255, 255, 0.95)",
            "padding": "20px",
            "maxWidth": "60%",
        }

        if loading_info_data["is_loading"] and loading_info_data["message_for_ai"]=="INITIAL_ANALYSIS_REQUEST":
            progress_value = animation_info_data["current_progress"]
            step_text = animation_info_data["current_step_text"]
            status_text = (
                "Loading your selected data..."
            )

            return open_style, -1, progress_value, step_text, status_text
        elif loading_info_data["is_loading"]: 
            progress_value = animation_info_data["current_progress"]
            step_text = animation_info_data["current_step_text"]
            status_text = (
                "Analyzing your request..."
            )

            return open_style, -1, progress_value, step_text, status_text
        else:
            # Not loading or loading finished
            return hide_style, 0, 0, "Analysis complete", "Ready"

    @app.callback(
        Output("animation-info-store", "data", allow_duplicate=True),
        [Input("thinking-interval", "n_intervals")],
        [State("loading-info-store", "data"), State("animation-info-store", "data")],
        prevent_initial_call=True,
    )
    def update_loading_animation_state(
        n_intervals, current_loading_info, current_animation_info
    ):
        if not current_loading_info["is_loading"]:
            return no_update  # Should not happen if interval is properly disabled

        if current_loading_info["message_for_ai"]=="INITIAL_ANALYSIS_REQUEST":
            step_messages = LOADING_STEPS.copy()
        else:
            step_messages = THINKING_STEPS.copy()

        # Simulate progress
        # current_progress = current_loading_info['current_progress']
        # Instead of direct increment, let's calculate based on steps.
        # This provides a more controlled progression through steps.

        num_steps = len(step_messages)
        # Calculate which step we should be on based on n_intervals, assuming each step takes some ticks
        # This is a simple way to advance; you can make it more sophisticated
        ticks_per_step = 10  # For example, 5 interval ticks per thinking step
        current_step_index = (n_intervals // ticks_per_step) % num_steps

        new_progress = min(
            99, int(((current_step_index + 1) / num_steps) * 100)
        )  # Cap at 99 until AI confirms completion

        # If it's an initial analysis, it might take longer, or you might have different steps.
        # This example uses the same steps for all.

        if (
            current_animation_info["current_progress"] >= new_progress
            and current_animation_info["current_step_text"]
            == step_messages[current_step_index]
        ):
            return no_update  # Avoid unnecessary updates if progress hasn't changed meaningfully

        updated_animation_info = current_animation_info.copy()
        updated_animation_info["current_progress"] = new_progress
        updated_animation_info["current_step_text"] = step_messages[current_step_index]

        return updated_animation_info

    @app.callback(
        [
            Output("chat-messages", "children", allow_duplicate=True),
            Output("chat-user-input", "value"),
            Output("chat-counter", "data"),
            Output("chat-alert-message", "is_open"),
            Output("chat-alert-message", "children"),
            Output("chat-alert-message", "color"),
            Output("loading-info-store", "data", allow_duplicate=True),
            Output("animation-info-store", "data", allow_duplicate=True),
        ],
        [
            Input("chat-send-button", "n_clicks"),
            Input("chat-user-input", "n_submit"),
        ],
        [
            State("chat-user-input", "value"),
            State("chat-messages", "children"),
            State("chat-counter", "data"),
            State("loading-info-store", "data"),
            State("animation-info-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def send_user_message(
        send_clicks,
        enter_pressed,
        user_input,
        chat_messages,
        chat_count,
        current_loading_info,
        current_animation_info,
    ):
        if not isinstance(chat_messages, list):
            chat_messages = [chat_messages] if chat_messages else []

        if (not send_clicks and not enter_pressed) or not user_input:
            return (
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
                no_update,
            )

        if chat_count >= MAX_CHATS:
            return (
                no_update,
                user_input,
                no_update,
                True,
                f"You have reached the maximum of {MAX_CHATS} messages.",
                "warning",
                no_update,
                no_update,
            )

        logger.info("User sending message. Preparing to trigger AI.")
        user_message_component = ChatMessage.create_message(
            role="user", text=user_input
        )
        chat_messages.append(user_message_component)
        new_chat_count = chat_count + 1

        new_loading_info = {
            "is_loading": True,
            "ai_task_active": True,  # Signal that a task is ready
            "message_for_ai": user_input,
        }

        new_animation_info = {
            "current_step_text": THINKING_STEPS[0],
            "current_progress": 0,
            "animation_instance": current_animation_info.get("animation_instance", 0)
            + 1,
        }

        return (
            chat_messages,
            "",
            new_chat_count,
            False,
            no_update,
            no_update,
            new_loading_info,
            new_animation_info,
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
            Output("loading-info-store", "data", allow_duplicate=True),
            Output("animation-info-store", "data", allow_duplicate=True),
        ],
        [Input("new-user-selection", "data")],
        [
            State("chat-modal", "is_open"),
            State("chat-messages", "children"),
            State("chat-allowed", "data"),
            State("loading-info-store", "data"),
            State("animation-info-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def trigger_initial_ai_analysis(
        modal_is_open,
        new_user_selection,
        current_messages,
        chat_allowed,
        current_loading_info,
        current_animation_info,
    ):
        if not isinstance(current_messages, list):
            current_messages = [current_messages] if current_messages else []

        if (
            modal_is_open
            and new_user_selection
            and chat_allowed
            and (
                not current_messages or len(current_messages) <= 1
            )  # Ensure it's a fresh chat
            and not current_loading_info[
                "is_loading"
            ]  # Only trigger if not already loading
        ):
            logger.info(
                "Chat modal opened for new selection. Triggering initial AI analysis."
            )
            initial_load_info = {
                "is_loading": True,
                "ai_task_active": True,  # Signal that a task is ready
                "message_for_ai": "INITIAL_ANALYSIS_REQUEST",
            }
            initial_animation_info = {
                "current_step_text": THINKING_STEPS[0],
                "current_progress": 0,
                "animation_instance": current_loading_info.get("animation_instance", 0)
                + 1,
            }
            return initial_load_info, initial_animation_info
        return no_update, no_update

    @app.callback(
        [
            Output("chat-messages", "children", allow_duplicate=True),
            Output("agent-session-id", "data", allow_duplicate=True),
            Output(
                "loading-info-store", "data", allow_duplicate=True
            ),  # To set is_loading: False
            Output("new-user-selection", "data", allow_duplicate=True),
            Output("animation-info-store", "data", allow_duplicate=True),
        ],
        [Input("loading-info-store", "data")],  # Primary trigger
        [
            State("animation-info-store", "data"),
            State("chat-modal", "is_open"),  # Still useful for context
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
            State("new-user-selection", "data"),  # Current value
        ],
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=5)  # Ensure output_count matches
    def get_ai_response_and_manage_state(
        loading_info_input,  # This is the data from loading-info-store that triggered the callback
        animation_info_state,
        modal_is_open_state,
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
        current_new_user_selection_state,
    ):
        if not loading_info_input or not loading_info_input.get("ai_task_active"):
            # Not a valid trigger for AI processing, or task already picked up by another instance.
            # Or is_loading became false for other reasons.
            # If is_loading is true but ai_task_active is false, it means we are just animating.
            return no_update, no_update, no_update, no_update, no_update

        if not isinstance(current_messages, list):
            current_messages = [current_messages] if current_messages else []

        ai_message_to_process = loading_info_input["message_for_ai"]
        new_user_selection_output = no_update  # Default to no_update
        store_update_after_pickup = loading_info_input.copy()
        store_update_after_pickup["ai_task_active"] = False

        try:
            if ai_message_to_process == "INITIAL_ANALYSIS_REQUEST":
                logger.info("Processing initial AI analysis request.")
                if not modal_is_open_state or not chat_allowed:
                    logger.warning(
                        "Initial analysis condition not met, aborting AI call."
                    )
                    final_loading_info = store_update_after_pickup.copy()
                    final_animation_info = animation_info_state.copy()
                    final_loading_info["is_loading"] = False
                    return (
                        no_update,
                        no_update,
                        final_loading_info,
                        no_update,
                        no_update,
                    )

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
                    final_loading_info = store_update_after_pickup.copy()
                    final_loading_info["is_loading"] = False
                    logger.warning("Data fetch for initial analysis returned empty.")
                    error_message_component = ChatMessage.create_message(
                        role="ai",
                        text="Sorry, I could not retrieve data for the initial analysis.",
                    )
                    return (
                        current_messages + [error_message_component],
                        session_id,
                        final_loading_info,
                        no_update,
                        no_update,
                    )

                new_session_id, initial_message_component = (
                    ChatService.start_ai_session(df=df)
                )
                if not new_session_id or not initial_message_component:
                    # Handle error
                    final_loading_info = store_update_after_pickup.copy()
                    final_loading_info["is_loading"] = False
                    logger.error("Failed to start AI session for initial analysis.")
                    error_message_component = ChatMessage.create_message(
                        role="ai",
                        text="Sorry, I could not initialize the analysis session.",
                    )
                    return (
                        current_messages + [error_message_component],
                        None,
                        final_loading_info,
                        no_update,
                        no_update,
                    )

                logger.info(f"Initial AI session started ({new_session_id}).")
                updated_messages = current_messages + [initial_message_component]
                new_user_selection_output = False
                final_loading_info = store_update_after_pickup.copy()
                final_animation_info = animation_info_state.copy()
                final_loading_info["is_loading"] = False
                final_loading_info["message_for_ai"] = (
                    None  # Clear the processed message
                )
                final_animation_info["current_progress"] = 100
                final_animation_info["current_step_text"] = "Analysis complete"
                return (
                    updated_messages,
                    new_session_id,
                    final_loading_info,
                    new_user_selection_output,
                    final_animation_info,
                )

            elif isinstance(ai_message_to_process, str):  # User message
                logger.info(
                    f"Processing user message for AI: '{ai_message_to_process}'"
                )
                if not session_id:
                    # Handle error
                    final_loading_info = store_update_after_pickup.copy()
                    final_animation_info = animation_info_state.copy()
                    final_loading_info["is_loading"] = False
                    logger.error("Cannot process user message: No active AI session.")
                    error_message_component = ChatMessage.create_message(
                        role="ai", text="Sorry, there's no active analysis session."
                    )
                    return (
                        current_messages + [error_message_component],
                        session_id,
                        final_loading_info,
                        no_update,
                        final_animation_info,
                    )

                ai_response_message_component = ChatService.invoke_ai_agent(
                    user_input=ai_message_to_process, session_id=session_id
                )
                logger.info("Received subsequent AI response.")
                updated_messages = current_messages + [ai_response_message_component]

                final_loading_info = store_update_after_pickup.copy()
                final_animation_info = animation_info_state.copy()
                final_loading_info["is_loading"] = False
                final_loading_info["message_for_ai"] = (
                    None  # Clear the processed message
                )
                final_animation_info["current_progress"] = 100
                final_animation_info["current_step_text"] = (
                    "Response ready"  # Or similar
                )
                return (
                    updated_messages,
                    session_id,
                    final_loading_info,
                    no_update,
                    final_animation_info,
                )
            else:
                logger.warning(
                    f"Unknown AI message type in loading_info: {ai_message_to_process}"
                )
                final_loading_info = store_update_after_pickup.copy()
                final_loading_info["is_loading"] = False
                return no_update, no_update, final_loading_info, no_update, no_update

        except Exception as e:
            logger.error(f"Error during AI processing: {e}", exc_info=True)
            error_message_component = ChatMessage.create_message(
                role="ai", text=f"Sorry, an error occurred during analysis: {e}"
            )
            updated_messages_on_error = current_messages + [error_message_component]

            final_loading_info_on_error = store_update_after_pickup.copy()
            final_loading_info_on_error["is_loading"] = False
            final_loading_info_on_error["message_for_ai"] = (
                None  # Clear message on error too
            )
            return (
                updated_messages_on_error,
                session_id,
                final_loading_info_on_error,
                no_update,
                no_update,
            )

    @app.callback(
        [
            Output("chat-messages", "children", allow_duplicate=True),
            Output("new-user-selection", "data"),
            Output("chat-selection-hash", "data"),
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
    def clear_chat_window(
        modal_is_open,
        previous_selection_hash,
        current_messages,
        shapes,
        asset_overlays,
        selected_region,
        selected_hazard,
        selected_ssp,
        selected_decade,
        selected_month,
    ):
        selected_hash = generate_robust_hash(
            shapes,
            asset_overlays,
            selected_region,
            selected_hazard,
            selected_decade,
            selected_month,
            selected_ssp,
        )
        new_selection = selected_hash != previous_selection_hash

        if new_selection:
            current_messages = [current_messages[0]]

        return (current_messages, new_selection, selected_hash)
