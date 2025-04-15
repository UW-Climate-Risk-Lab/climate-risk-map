import dash_bootstrap_components as dbc

from dash import html

from config.ui_config import PRIMARY_COLOR
from config.chat.messages import ChatMessage


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
                [
                    # Chat message container with scrollable area for messages
                    # We simulate a chat by appending new messages to 'ai-chat-messages' children
                    html.Div(
                        id="chat-messages",
                        children=[
                            # Initial welcome message
                            ChatMessage.create_message(
                                role="ai",
                                text="👋 Hi there! I'm your Climate Risk AI Assistant. I am currently generating an intitial analysis of your selected area, which may take a moment. You can then ask follow up questions. ",
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
                        html.Div(id="chat-loading-placeholder"),
                        id="chat-loading-spinner",
                        delay_show=100,
                        color=PRIMARY_COLOR,
                        fullscreen=True,
                        fullscreen_style={"backgroundColor": "transparent"},
                    ),
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
