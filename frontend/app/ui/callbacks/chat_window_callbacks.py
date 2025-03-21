import logging
import time
from dash import html, Input, Output, no_update, State, dcc, callback_context
import plotly.graph_objects as go

from config.ui_config import UIConfig
from config.chat.messages import ChatMessage

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
            State("ai-chat-messages", "children")
        ],
        prevent_initial_call=True,
    )
    def send_message(send_clicks, enter_pressed, user_input, chat_messages):
        """Handle sending user messages and receiving AI responses"""

        if not isinstance(chat_messages, list):
            chat_messages = [chat_messages]

        if not user_input or (not send_clicks and not enter_pressed):
            return chat_messages, ""
        
        # Add user message to chat
        user_message = ChatMessage.create_message(role="user", text=user_input)
        
        # PLACEHOLDER: Here you will add the actual AI service call
        # For now, we just return a placeholder response to show the design
    
        
        chat_messages.append(user_message)
        
        # Placeholder for the actual AI response (this would come from your AWS Bedrock agent)
        # In the actual implementation, you would make an async call to your AI service
        
        return chat_messages, ""
    
    @app.callback(
        
        [Output("ai-chat-messages", "children"), Output("ai-loading-placeholder", "children")],
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
            return chat_messages, ""
        
        # Create a placeholder visualization
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

        # Add AI response with the visualization
        ai_message = ChatMessage.create_message(role="ai", text="Here is your detailed analysis cndsjinhcdkjsbncjkhdsbncjkdsnjkcndskjcjn jkdsncjkdsnjkcdn sjkncdjksnjcds kjnccndjsn cjkdsncjkdsnjkcndsjkncjkdsncjkd snjkcnjkdsncjkdsnkjcndjksncjkdsncjkdsnjkcndsjkcndjksncjkdsncjkdsnjkcndsjkcnj dksncjkdsncjkdsncjkdsncjkdsncjkdsnckjdsnckdsnckjdjnskcjndsknc cdnsjkcndsjk c dsjk", fig=fig)

        # Add the AI response to the chat messages
        chat_messages.append(ai_message)
        time.sleep(2)
        return chat_messages, ""