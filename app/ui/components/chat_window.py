import dash_bootstrap_components as dbc

from dash import html

from config.ui_config import PRIMARY_COLOR, SECONDARY_COLOR
from config.chat.messages import ChatMessage


def create_climate_thinking_indicator():
    """
    Create a floating climate-themed "thinking" indicator that hovers over the chat window
    while the AI is processing data.
    """
    return html.Div(
        id="climate-thinking-indicator",
        className="climate-thinking-container",
        style={
            "display": "none",  # Initially hidden
            "position": "absolute",  # Absolute positioning for floating effect
            "top": "50%",  # Center vertically
            "left": "50%",  # Center horizontally
            "transform": "translate(-50%, -50%)",  # Center adjustment
            "zIndex": "1500",  # Higher than modal (1050) to float on top
            "borderRadius": "10px",
            "boxShadow": "0 4px 12px rgba(0,0,0,0.25)",  # Stronger shadow for floating effect
            "backgroundColor": "rgba(255, 255, 255, 0.95)",  # Slightly transparent background
            "maxWidth": "60%",  # Don't take up too much space
        },
        children=[
            html.Div(
                className="climate-thinking-animation",
                style={
                    "padding": "15px",
                    "display": "flex",
                    "flexDirection": "column",
                    "gap": "10px",
                    "border": f"1px solid {SECONDARY_COLOR}",
                    "borderRadius": "10px",
                },
                children=[
                    # Thinking header
                    html.Div(
                        [
                            html.I(className="fa fa-cloud me-2"),
                            html.Span(
                                id="thinking-status-text",
                                children="Analyzing climate data...",
                            ),
                        ],
                        style={"fontWeight": "bold"},
                    ),
                    # Climate data visualization placeholder
                    html.Div(
                        className="climate-viz-placeholder",
                        children=[
                            html.Div(
                                className="climate-wave wave1",
                                style={"backgroundColor": "#69b3a2"},
                            ),
                            html.Div(
                                className="climate-wave wave2",
                                style={"backgroundColor": "#3498db"},
                            ),
                            html.Div(
                                className="climate-wave wave3",
                                style={"backgroundColor": "#f39c12"},
                            ),
                        ],
                        style={
                            "height": "40px",
                            "width": "100%",
                            "position": "relative",
                            "marginTop": "5px",
                            "marginBottom": "5px",
                            "overflow": "hidden",
                            "borderRadius": "5px",
                            "backgroundColor": "#f5f5f5",
                        },
                    ),
                    # Progress bar and step indicator
                    html.Div(
                        [
                            dbc.Progress(
                                id="thinking-progress",
                                value=0,
                                color=PRIMARY_COLOR,
                                className="mb-2",
                                style={"height": "6px"},
                            ),
                            html.Div(
                                id="thinking-step-indicator",
                                children="Preparing analysis...",
                                style={"fontSize": "12px", "color": "#6c757d"},
                            ),
                        ]
                    ),
                ],
            )
        ],
    )


def create_ai_analysis_modal():
    """Create the AI analysis modal with chat interface

    This is the chat window pop up, which is initially closed.

    Returns:
        dbc.Modal: Modal component with chat interface
    """
    return dbc.Modal(
        id="chat-modal",
        is_open=False,
        size="lg",
        scrollable=True,
        centered=True,
        style={"zIndex": 1050},
        children=[
            dbc.ModalHeader(
                dbc.ModalTitle(
                    [html.I(className="fa fa-robot me-2"), "Climate Risk AI Assistant"],
                    style={"color": PRIMARY_COLOR},
                ),
                close_button=True,
                style={"background-color": "white"},
            ),
            dbc.ModalBody(
                [  # Climate thinking indicator
                    create_climate_thinking_indicator(),
                    # Chat message container with scrollable area for messages
                    # We simulate a chat by appending new messages to 'ai-chat-messages' children
                    html.Div(
                        id="chat-messages",
                        children=[
                            # Initial welcome message
                            ChatMessage.create_message(
                                role="ai",
                                text="👋 Hi there! I'm your Climate Risk AI Instructor. I am currently downloading the data of your selected area, which may take a moment. You can then ask follow up questions.",
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
                    # Keep this for callback compatibility
                    html.Div(id="chat-loading-placeholder"),
                    dbc.Alert(
                        id="chat-alert-message",
                        color="danger",
                        is_open=False,
                        duration=3000,
                    ),
                    # Input area for user messages
                    dbc.InputGroup(
                        [
                            dbc.Textarea(
                                id="chat-user-input",
                                placeholder="Ask me about climate risks in your selected region...",
                                rows=2,
                                style={
                                    "resize": "none",
                                    "borderRadius": "10px 0 0 10px",
                                },
                            ),
                            dbc.InputGroupText(
                                html.Button(
                                    "↑",
                                    id="chat-send-button",
                                    className="btn",
                                    style={
                                        "border": "none",
                                        "backgroundColor": PRIMARY_COLOR,
                                        "color": "white",
                                    },
                                ),
                                style={
                                    "backgroundColor": "white",
                                    "borderRadius": "0 10px 10px 0",
                                    "border": "1px solid #ced4da",
                                    "borderLeft": "none",
                                },
                            ),
                        ],
                    ),
                ]
            ),
            dbc.ModalFooter(
                [
                    html.Small(
                        "Powered by AWS Bedrock", className="text-muted me-auto"
                    ),
                    dbc.Button(
                        "Close",
                        id="chat-modal-close",
                        className="ms-auto",
                        style={
                            "backgroundColor": PRIMARY_COLOR,
                            "color": "white",
                            "borderRadius": "10px",
                        },
                    ),
                ],
                style={"backgroundColor": "#f8f9fa"},
            ),
        ],
    )
