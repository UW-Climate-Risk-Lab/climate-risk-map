import logging
import time
from dash import html, Input, Output, no_update, State, dcc, callback_context
import plotly.graph_objects as go

from config.ui_config import UIConfig

logger = logging.getLogger(__name__)


def register_ai_analysis_callbacks(app):
    """Register callbacks for AI analysis modal and chat interface
    
    Args:
        app: Dash application instance
    """
    
    @app.callback(
        Output("ai-analysis-modal", "is_open"),
        [
            Input("ai-analysis-btn", "n_clicks"),
            Input("ai-modal-close", "n_clicks")
        ],
        [State("ai-analysis-modal", "is_open")],
        prevent_initial_call=True,
    )
    def toggle_modal(open_clicks, close_clicks, is_open):
        """Toggle the visibility of the AI analysis modal"""
        ctx = callback_context
        if not ctx.triggered:
            return is_open
        
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        if trigger_id == "ai-analysis-btn" and open_clicks:
            return True
        elif trigger_id == "ai-modal-close" and close_clicks:
            return False
        return is_open
    
    @app.callback(
        [
            Output("ai-chat-messages", "children", allow_duplicate=True),
            Output("ai-user-input", "value"),
        ],
        [
            Input("ai-send-button", "n_clicks"),
            Input("ai-user-input", "n_submit"),
        ],
        [
            State("ai-user-input", "value"),
            State("ai-chat-messages", "children"),
            # Add other state variables needed for AI analysis
            State("region-select-dropdown", "value"),
            State("hazard-indicator-dropdown", "value"),
            State("ssp-dropdown", "value"),
            State("decade-slider", "value"),
            State("month-slider", "value"),
        ],
        prevent_initial_call=True,
    )
    def send_message(send_clicks, enter_pressed, user_input, chat_messages, 
                    region, hazard, ssp, decade, month):
        """Handle sending user messages and receiving AI responses"""

        if not isinstance(chat_messages, list):
            chat_messages = [chat_messages]

        if not user_input or (not send_clicks and not enter_pressed):
            return chat_messages, ""
        
        # Add user message to chat
        user_message_div = html.Div(
            [
                html.Div(
                    user_input,
                    className="user-message",
                    style={
                        "backgroundColor": UIConfig.PRIMARY_COLOR,
                        "color": "white",
                        "borderRadius": "10px 10px 0 10px",
                        "padding": "10px 15px",
                        "marginLeft": "auto",
                        "marginBottom": "5px",
                        "maxWidth": "80%",
                        "boxShadow": "0 1px 2px rgba(0,0,0,0.1)",
                    }
                )
            ],
            className="message-container",
            style={"display": "flex", "justifyContent": "flex-end", "marginBottom": "15px"}
        )
        
        # PLACEHOLDER: Here you will add the actual AI service call
        # For now, we just return a placeholder response to show the design
    
        
        chat_messages.append(user_message_div)
        
        # Placeholder for the actual AI response (this would come from your AWS Bedrock agent)
        # In the actual implementation, you would make an async call to your AI service
        
        return chat_messages, ""
    
    @app.callback(
        
        Output("ai-chat-messages", "children"),
        [Input("ai-user-input", "value")],
        [State("ai-chat-messages", "children")],
        prevent_initial_call=True,
    )
    def simulate_ai_response(_, chat_messages):
        """Simulate an AI response (placeholder for the actual AI service call)"""
        # This is just a placeholder to demonstrate the UI
        # In the actual implementation, this would be triggered by the AI service response
        
        if not isinstance(chat_messages, list):
            chat_messages = [chat_messages]

        if len(chat_messages) % 2 != 0:
            # Only add AI response if the last message was from the user
            return chat_messages
        
        # Create a placeholder visualization
        fig = go.Figure(data=go.Scatter(x=[1, 2, 3, 4], y=[10, 11, 12, 13]))
        fig.update_layout(
            title="Sample Climate Risk Analysis",
            xaxis_title="Time Period",
            yaxis_title="Risk Level",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        
        # Add AI response with the visualization
        ai_message_div = html.Div(
            [
                html.Div(
                    [
                        html.P(
                            "Based on your selected region and hazard indicators, here's my analysis:",
                            style={"marginBottom": "10px"}
                        ),
                        dcc.Graph(
                            figure=fig,
                            config={"displayModeBar": False},
                            style={"marginBottom": "10px"}
                        ),
                        html.P(
                            "This visualization shows the projected trend in climate risks over time. "
                            "You can see that risk levels are expected to increase gradually. "
                            "Would you like more specific insights about this data?"
                        )
                    ],
                    className="ai-message",
                    style={
                        "backgroundColor": "white",
                        "borderRadius": "10px 10px 10px 0",
                        "padding": "10px 15px",
                        "marginRight": "auto",
                        "marginBottom": "5px",
                        "maxWidth": "80%",
                        "boxShadow": "0 1px 2px rgba(0,0,0,0.1)",
                        "border": f"1px solid {UIConfig.SECONDARY_COLOR}",
                    }
                )
            ],
            className="message-container",
            style={"display": "flex", "justifyContent": "flex-start", "marginBottom": "15px"}
        )
        
        # Add the AI response to the chat messages
        chat_messages.append(ai_message_div)
        time.sleep(5)
        return chat_messages