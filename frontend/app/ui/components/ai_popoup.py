import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State
import plotly.graph_objects as go

from config.ui_config import UIConfig


def create_ai_analysis_modal():
    """Create the AI analysis modal with chat interface
    
    Returns:
        dbc.Modal: Modal component with chat interface
    """
    return dbc.Modal(
        [
            dbc.ModalHeader(
                dbc.ModalTitle(
                    [
                        html.I(className="fa fa-robot me-2"),
                        "Climate Risk AI Assistant"
                    ],
                    style={"color": UIConfig.PRIMARY_COLOR}
                ),
                close_button=True,
                style={"background-color": "white"}
            ),
            dbc.ModalBody(
                [
                    # Chat message container with scrollable area for messages
                    html.Div(
                        id="ai-chat-messages",
                        children=[
                            # Initial welcome message
                            html.Div(
                                [
                                    html.Div(
                                        "👋 Hi there! I'm your Climate Risk AI Assistant. I can help analyze climate data for your selected region and hazard indicators. What would you like to know?",
                                        className="ai-message",
                                    )
                                ],
                                className="message-container",
                            ),
                        ],
                        style={
                            "height": "400px",
                            "overflowY": "auto",
                            "display": "flex",
                            "flexDirection": "column",
                            "padding": "10px",
                            "backgroundColor": "#f5f5f5",
                            "borderRadius": "10px",
                            "marginBottom": "15px",
                        },
                    ),
                    
                    # Loading indicator for AI responses
                    dbc.Spinner(
                        html.Div(id="ai-loading-placeholder"),
                        id="ai-loading-spinner",
                        type="grow",
                        color=UIConfig.PRIMARY_COLOR,
                        fullscreen=False,
                        fullscreen_style={"backgroundColor": "transparent"},
                    ),
                    
                    # Input area for user messages
                    dbc.InputGroup(
                        [
                            dbc.Textarea(
                                id="ai-user-input",
                                placeholder="Ask me about climate risks in your selected region...",
                                rows=2,
                                style={
                                    "resize": "none",
                                    "borderRadius": "10px 0 0 10px",
                                }
                            ),
                            dbc.InputGroupText(
                                html.Button(
                                    html.I(className="fa fa-paper-plane"),
                                    id="ai-send-button",
                                    className="btn",
                                    style={
                                        "border": "none", 
                                        "backgroundColor": UIConfig.PRIMARY_COLOR,
                                        "color": "white",
                                    }
                                ),
                                style={
                                    "backgroundColor": "white",
                                    "borderRadius": "0 10px 10px 0",
                                    "border": "1px solid #ced4da",
                                    "borderLeft": "none",
                                }
                            ),
                        ],
                    ),
                ]
            ),
            dbc.ModalFooter(
                [
                    html.Small(
                        "Powered by AWS Bedrock",
                        className="text-muted me-auto"
                    ),
                    dbc.Button(
                        "Close",
                        id="ai-modal-close",
                        className="ms-auto",
                        style={
                            "backgroundColor": UIConfig.PRIMARY_COLOR,
                            "color": "white",
                            "borderRadius": "10px",
                        }
                    ),
                ],
                style={"backgroundColor": "#f8f9fa"}
            ),
        ],
        id="ai-analysis-modal",
        is_open=False,
        size="lg",
        scrollable=True,
        centered=True,
        style={"zIndex": 1050},
    )





# Add custom CSS for the chat interface
def get_ai_chat_css():
    """Get custom CSS for AI chat interface
    
    Returns:
        str: CSS rules to be included in the app
    """
    return """
    /* Custom CSS for AI chat interface */
    .ai-message {
        background-color: white;
        border-radius: 10px 10px 10px 0;
        padding: 10px 15px;
        margin-right: auto;
        margin-bottom: 5px;
        max-width: 80%;
        box-shadow: 0 1px 2px rgba(0,0,0,0.1);
    }
    
    .user-message {
        background-color: #39275B;
        color: white;
        border-radius: 10px 10px 0 10px;
        padding: 10px 15px;
        margin-left: auto;
        margin-bottom: 5px;
        max-width: 80%;
        box-shadow: 0 1px 2px rgba(0,0,0,0.1);
    }
    
    .message-container {
        display: flex;
        margin-bottom: 15px;
    }
    
    #ai-send-button:hover {
        color: #4B2E83;
    }
    
    #ai-user-input:focus {
        box-shadow: 0 0 0 0.25rem rgba(57, 39, 91, 0.25);
        border-color: #4B2E83;
    }
    """