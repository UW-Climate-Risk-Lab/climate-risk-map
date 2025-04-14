"""
Here we set up some simple classes to represent messages in our application.
A message in our chat window consists of some content and a container for it,
which has specific styling. We create this config to package up the html components
into a single object and set different styling based on the role of the message.

This makes messages easier to create in the chat window application code, and ensures
they are generated with a consistent format.

"""

from abc import ABC, abstractmethod

from dash import html, dcc
from plotly.graph_objects import Figure

from config.ui_config import PRIMARY_COLOR, SECONDARY_COLOR



class ChatMessage:
    """Factory to generate chat message with provided role

    This will be called in the chat window ui components and callbacks code.
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
            "border": f"1px solid {SECONDARY_COLOR}",
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

        # Passing in fig can give weird error 
        # "NSWindow should only be instantiated on the main thread!" 
        # is occurring because Matplotlib is trying to create a GUI window 
        # outside the main thread, which isn't allowed in macOS

        # Possible fix chat_service.py
        # # Instead of creating a Matplotlib figure, encode as base64
        # if type == "image/png":
        #     b64_image = base64.b64encode(bytes_data).decode('utf-8')
        #     image_src = f"data:image/png;base64,{b64_image}"
        #     # Pass the image source to the message
        #     message = ChatMessage.create_message(role="ai", text=text, image_src=image_src)

        # At the top of chat_service.py, add:
        # import matplotlib
        # matplotlib.use('Agg')  # Use non-interactive backend
        # import matplotlib.pyplot as plt
        # import base64

        # Add figure if present
        # if fig:
        #     message_content.append(dcc.Graph(
        #         figure=fig,
        #         config={"displayModeBar": False},
        #         style={"marginBottom": "10px"},
        #     ))
        
        # # Add base64 image if present
        # if image_src:
        #     message_content.append(html.Img(
        #         src=image_src,
        #         style={"maxWidth": "100%", "marginBottom": "10px"}
        #     ))
        
        if fig:
            message = html.Div(
                [
                    dcc.Markdown(
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
            message = html.Div(
                [
                    dcc.Markdown(
                        text,
                        style={"marginBottom": "10px"},
                    ),
                ],
                className="ai-message",
                style=self._message_style,
            )

        message_container.children.append(message)

        return message_container


class UserMessage(Message):
    def __init__(self):
        super().__init__()

        self._message_style = {
            "backgroundColor": PRIMARY_COLOR,
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
