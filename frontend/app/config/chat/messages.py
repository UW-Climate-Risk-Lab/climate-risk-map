from abc import ABC, abstractmethod

from dash import html, dcc
from plotly.graph_objects import Figure

from config.ui_config import UIConfig



class ChatMessage:
    """Factory to generate chat message with provided role
    """
    @staticmethod
    def create_message(role, text, **kwargs):
        if role == "ai":
            return AiMessage().create_message(text, **kwargs)
        elif role == "user":
            return UserMessage().create_message(text, **kwargs)
        else:
            raise ValueError(f"Unknown message role type: {role}")

class Message(ABC):

    @abstractmethod
    def create_message(self, text: str, **kwargs):
        pass

class AiMessage(Message):

    def __init__(self):
        super().__init__()

        self._message_style = {
            "backgroundColor": "white",
            "borderRadius": "10px 10px 10px 0",
            "padding": "10px 15px",
            "marginRight": "auto",
            "marginBottom": "5px",
            "maxWidth": "80%",
            "boxShadow": "0 1px 2px rgba(0,0,0,0.1)",
            "border": f"1px solid {UIConfig.SECONDARY_COLOR}",
        }
        self._message_container_style = {
            "display": "flex",
            "justifyContent": "flex-start",
            "marginBottom": "15px",
        }

    def create_message(self, text: str, fig: Figure = None):

        message_container = html.Div(
            children=list(),
            className="ai-message-container",
            style=self._message_container_style,
        )

        if fig:
            message = html.Div(
                [
                    html.P(
                        text,
                        style={"marginBottom": "10px"},
                    ),
                    dcc.Graph(
                        figure=fig,
                        config={"displayModeBar": False},
                        style={"marginBottom": "10px"},
                    ),
                ],
                className="ai-message",
                style=self._message_style,
            )
        else:
            message = html.Div(text, className="ai-message", style=self._message_style)

        message_container.children.append(message)

        return message_container


class UserMessage(Message):
    def __init__(self):
        super().__init__()

        self._message_style = {
            "backgroundColor": UIConfig.PRIMARY_COLOR,
            "color": "white",
            "borderRadius": "10px 10px 0 10px",
            "padding": "10px 15px",
            "marginLeft": "auto",
            "marginBottom": "5px",
            "maxWidth": "80%",
            "boxShadow": "0 1px 2px rgba(0,0,0,0.1)",
        }

        self._message_container_style = {
            "display": "flex",
            "justifyContent": "flex-end",
            "marginBottom": "15px",
        }

    def create_message(self, text: str, **kwargs):

        message_container = html.Div(
            children=list(),
            className="user-message-container",
            style=self._message_container_style,
        )

        message = html.Div(
                    text,
                    className="user-message",
                    style=self._message_style
                )
        
        message_container.children.append(message)

        return message_container
