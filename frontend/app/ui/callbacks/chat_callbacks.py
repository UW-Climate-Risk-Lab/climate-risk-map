import logging

from dash import Input, Output, no_update, State, callback_context

from config.settings import MAX_CHATS
from config.ui_config import UIConfig
from config.map_config import MapConfig
from config.chat.messages import ChatMessage

from services.download_service import DownloadService
from services.chat_service import ChatService
from utils.error_utils import handle_callback_error

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
            return is_open

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

        if not user_input or (not send_clicks and not enter_pressed):
            return chat_messages, "", no_update, no_update

        if chat_count > MAX_CHATS:
            return (
                chat_messages,
                no_update,
                chat_count,
                True,
                "You have exceeded the max number of chats allowed!",
            )

        # Add user message to chat
        user_message = ChatMessage.create_message(role="user", text=user_input)

        chat_messages.append(user_message)

        return chat_messages, "", chat_count + 1, False, no_update

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
    @handle_callback_error(output_count=6)
    def prep_chat(
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
            chat_prep_config = ChatService.create_chat_config(
                shapes=shapes,
                asset_overlays=selected_assets,
                region_name=selected_region,
                hazard_name=selected_hazard,
                ssp=selected_ssp,
                decade=selected_decade,
                month=selected_month,
            )

            return (
                chat_prep_config.chat_allowed,
                0,
                chat_prep_config.alert_message,
                chat_prep_config.alert_message_is_open,
                chat_prep_config.alert_message_color,
            )

        return no_update, 0, no_update, no_update, no_update

    @app.callback(
        [
            Output("chat-messages", "children"),
            Output("chat-loading-placeholder", "children"),
            Output("agent-session-id", "data"),
        ],
        [Input("chat-user-input", "value"),
         Input("chat-modal", "is_open")],
        [
            State("chat-messages", "children"),
            State(MapConfig.BASE_MAP_COMPONENT["drawn_shapes_layer"]["id"], "geojson"),
            State(MapConfig.BASE_MAP_COMPONENT["asset_layer"]["id"], "overlays"),
            State("region-select-dropdown", "value"),
            State("hazard-indicator-dropdown", "value"),
            State("ssp-dropdown", "value"),
            State("decade-slider", "value"),
            State("month-slider", "value"),
            State("agent-session-id", "data")
        ],
        prevent_initial_call=True,
    )
    def get_ai_response(
        user_input,
        open_chat,
        chat_messages,
        shapes,
        asset_overlays,
        selected_region,
        selected_hazard,
        selected_ssp,
        selected_decade,
        selected_month,
        session_id
    ):

        if not isinstance(chat_messages, list):
            chat_messages = [chat_messages]

        if not open_chat:
            return chat_messages, "", no_update

        if len(chat_messages) == 1 and open_chat:
            session_id = ChatService.create_session_id()
            df = DownloadService.get_download(
                shapes=shapes,
                asset_overlays=asset_overlays,
                region_name=selected_region,
                hazard_name=selected_hazard,
                ssp=selected_ssp,
                decade=selected_decade,
                month=selected_month,
            )
            response = ChatService.initial_agent_request(df=df, session_id=session_id)
        else:
            response = ChatService.invoke_ai(user_input=user_input, session_id=session_id)

        if len(chat_messages) % 2 == 0:
            # Only add AI response if the last message was from the user
            response = ChatService.invoke_ai(user_input=user_input, session_id=session_id)
    
        chat_messages.append(response)

        return chat_messages, "", session_id
